/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.tinkerpop.gremlin.structure.util.Attachable
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory

// an init script that returns a Map allows explicit setting of global bindings.
def globals = [:]

static void cloneElements(final Graph original, final Graph clone) {
    original.vertices().forEachRemaining(v -> DetachedFactory.detach(v, true).attach(Attachable.Method.create(clone)));
    original.edges().forEachRemaining(e -> DetachedFactory.detach(e, true).attach(Attachable.Method.create(clone)));
}

// defines a sample LifeCycleHook that prints some output to the Gremlin Server console.
// note that the name of the key in the "global" map is unimportant.
globals << [hook : [
        onStartUp: { ctx ->
            ctx.logger.info("Executed once at startup of Gremlin Server.")
            modern.getBaseGraph().dropDatabase(graph, false)
            Graph tg = TinkerFactory.createModern()
            cloneElements(tg, modern)
        },
        onShutDown: { ctx ->
            ctx.logger.info("Executed once at shutdown of Gremlin Server.")
        }
] as LifeCycleHook]

// define the default TraversalSource to bind queries to - this one will be named "g".
globals << [g : traversal().withEmbedded(graph), o : traversal().withEmbedded(graph).withComputer()]
globals << [gmodern : traversal().withEmbedded(modern)]
