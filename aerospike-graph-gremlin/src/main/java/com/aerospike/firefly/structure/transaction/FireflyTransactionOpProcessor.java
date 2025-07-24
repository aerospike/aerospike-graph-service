package com.aerospike.firefly.structure.transaction;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Failure;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.op.session.Session;
import org.apache.tinkerpop.gremlin.server.op.session.SessionOpProcessor;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.TemporaryException;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.apache.tinkerpop.gremlin.process.traversal.GraphOp.TX_COMMIT;
import static org.apache.tinkerpop.gremlin.process.traversal.GraphOp.TX_ROLLBACK;

public class FireflyTransactionOpProcessor extends SessionOpProcessor {
    static private final Logger LOG = LoggerFactory.getLogger(FireflyTransactionOpProcessor.class);

    public FireflyTransactionOpProcessor() {
        super();
    }

    @Override
    protected void beforeProcessing(final Graph graph, final Context ctx) {
        LOG.warn("beforeProcessing on Thread: {}", Thread.currentThread().getId());
        if (graph != null) {
            ((FireflyGraph) graph).enterTransactionState();
        }
        super.beforeProcessing(graph, ctx);
    }

    @Override
    protected void onError(final Graph graph, final Context ctx) {
        LOG.warn("onError on Thread: {}", Thread.currentThread().getId());
        try {
            super.onError(graph, ctx);
        } finally {
            if (graph != null) {
                ((FireflyGraph) graph).exitTransactionState();
            }
        }
    }

    @Override
    protected void onTraversalSuccess(final Graph graph, final Context ctx) {
        LOG.warn("onTraversalSuccess on Thread: {}", Thread.currentThread().getId());
        try {
            super.onTraversalSuccess(graph, ctx);
        } finally {
            if (graph != null) {
                ((FireflyGraph) graph).exitTransactionState();
            }
        }
    }

    @Override
    protected void handleGraphOperation(final Bytecode bytecode, final Graph graph, final Context context) {
        final RequestMessage msg = context.getRequestMessage();
        final Session session = getSession(context, msg);
        if (graph.features().graph().supportsTransactions()) {
            if (TX_COMMIT.equals(bytecode) || TX_ROLLBACK.equals(bytecode)) {
                final boolean commit = TX_COMMIT.equals(bytecode);

                // there is no timeout on a commit/rollback
                submitToGremlinExecutor(context, 0, session, new FutureTask<>(() -> {
                    try {
                        ((FireflyGraph) graph).enterTransactionState();
                        if (graph.tx().isOpen()) {
                            if (commit)
                                graph.tx().commit();
                            else
                                graph.tx().rollback();
                        }

                        // write back a no-op for success
                        final Map<String, Object> attributes = generateStatusAttributes(
                                context.getChannelHandlerContext(), msg,
                                ResponseStatusCode.NO_CONTENT, Collections.emptyIterator(), context.getSettings());
                        context.writeAndFlush(ResponseMessage.build(msg)
                                .code(ResponseStatusCode.NO_CONTENT)
                                .statusAttributes(attributes)
                                .create());

                    } catch (Throwable t) {
                        onError(graph, context);
                        // if any exception in the chain is TemporaryException or Failure then we should respond with the
                        // right error code so that the client knows to retry
                        final Optional<Throwable> possibleSpecialException = determineIfSpecialException(t);
                        if (possibleSpecialException.isPresent()) {
                            final Throwable special = possibleSpecialException.get();
                            final ResponseMessage.Builder specialResponseMsg = ResponseMessage.build(msg).
                                    statusMessage(special.getMessage()).
                                    statusAttributeException(special);
                            if (special instanceof TemporaryException) {
                                specialResponseMsg.code(ResponseStatusCode.SERVER_ERROR_TEMPORARY);
                            } else if (special instanceof Failure) {
                                final Failure failure = (Failure) special;
                                specialResponseMsg.code(ResponseStatusCode.SERVER_ERROR_FAIL_STEP).
                                        statusAttribute(Tokens.STATUS_ATTRIBUTE_FAIL_STEP_MESSAGE, failure.format());
                            }
                            context.writeAndFlush(specialResponseMsg.create());
                        } else {
                            LOG.warn(String.format("Exception processing a Traversal on request [%s] to %s the transaction.",
                                    msg.getRequestId(), commit ? "commit" : "rollback"), t);
                            context.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR)
                                    .statusMessage(t.getMessage())
                                    .statusAttributeException(t).create());
                        }
                        if (t instanceof Error) {
                            //Re-throw any errors to be handled by and set as the result the FutureTask
                            throw t;
                        }
                    }

                    ((FireflyGraph) graph).exitTransactionState();
                    return null;
                }));
            } else {
                throw new IllegalStateException(String.format(
                        "Bytecode in request is not a recognized graph operation: %s", bytecode.toString()));
            }
        } else {
            throw Graph.Exceptions.transactionsNotSupported();
        }
    }

    private static void submitToGremlinExecutor(final Context context, final long seto, final Session session,
                                                final FutureTask<Void> evalFuture) {
        final Future<?> executionFuture = session.getGremlinExecutor().getExecutorService().submit(evalFuture);
        if (seto > 0) {
            // Schedule a timeout in the thread pool for future execution
            context.setTimeoutExecutor(context.getScheduledExecutorService().schedule(() -> {
                executionFuture.cancel(true);
                if (!context.getStartedResponse()) {
                    context.sendTimeoutResponse();
                }
            }, seto, TimeUnit.MILLISECONDS));
        }
    }
}
