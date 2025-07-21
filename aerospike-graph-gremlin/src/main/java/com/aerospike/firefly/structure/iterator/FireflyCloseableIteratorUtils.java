package com.aerospike.firefly.structure.iterator;

import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class FireflyCloseableIteratorUtils {

    public static final <S> CloseableIterator<S> of(final S a) {
        return new FireflyCloseableSingleIterator<>(a);
    }

    public static final <S> CloseableIterator<S> of(final S a, S b) {
        return new FireflyCloseableDoubleIterator<>(a, b);
    }

    public static final <S, E> CloseableIterator<E> map(final Iterator<S> iterator, final Function<S, E> function) {
        return new CloseableIterator<E>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public void remove() {
                iterator.remove();
            }

            @Override
            public E next() {
                return function.apply(iterator.next());
            }

            @Override
            public void close() {
                CloseableIterator.closeIterator(iterator);
            }
        };
    }


    // IteratorUtils concat implements AutoCloseable via MultiIterator.
    public static final <S> Iterator<S> concat(final Iterator<S>... iterators) {
        // Returns MultiIterator which implements AutoCloseable
        return IteratorUtils.concat(iterators);
    }

    public static final <S> CloseableIterator<S> filter(final Iterator<S> iterator, final Predicate<S> predicate) {
        return new CloseableIterator<S>() {
            private S nextResult = null;

            @Override
            public boolean hasNext() {
                if (null != this.nextResult) {
                    return true;
                } else {
                    advance();
                    return null != this.nextResult;
                }
            }

            @Override
            public void remove() {
                iterator.remove();
            }

            @Override
            public S next() {
                try {
                    if (null != this.nextResult) {
                        return this.nextResult;
                    } else {
                        advance();
                        if (null != this.nextResult)
                            return this.nextResult;
                        else
                            throw FastNoSuchElementException.instance();
                    }
                } finally {
                    this.nextResult = null;
                }
            }

            private final void advance() {
                this.nextResult = null;
                while (iterator.hasNext()) {
                    final S s = iterator.next();
                    if (predicate.test(s)) {
                        this.nextResult = s;
                        return;
                    }
                }
            }

            @Override
            public void close() {
                CloseableIterator.closeIterator(iterator);
            }
        };
    }

    public static final long count(final Iterator iterator) {
        long ix = 0;
        for (; iterator.hasNext(); ++ix) iterator.next();
        return ix;
    }

    public static final long count(final Iterable iterable) {
        return IteratorUtils.count(iterable.iterator());
    }

    public static <S> List<S> list(final Iterator<S> iterator) {
        return fill(iterator, new ArrayList<>());
    }

    public static <S> List<S> list(final Iterator<S> iterator, final Comparator comparator) {
        final List<S> l = list(iterator);
        Collections.sort(l, comparator);
        return l;
    }

    public static <S> Set<S> set(final Iterator<S> iterator) {
        return fill(iterator, new HashSet<>());
    }

    public static <S> CloseableIterator<S> limit(final Iterator<S> iterator, final int limit) {
        return new CloseableIterator<S>() {
            private int count = 0;

            @Override
            public boolean hasNext() {
                return iterator.hasNext() && this.count < limit;
            }

            @Override
            public void remove() {
                iterator.remove();
            }

            @Override
            public S next() {
                if (this.count++ >= limit)
                    throw FastNoSuchElementException.instance();
                return iterator.next();
            }

            @Override
            public void close() {
                CloseableIterator.closeIterator(iterator);
            }
        };
    }

    public static final <S extends Collection<T>, T> S fill(final Iterator<T> iterator, final S collection) {
        while (iterator.hasNext()) {
            collection.add(iterator.next());
        }
        return collection;
    }

    /**
     * Construct a {@link Stream} from an {@link Iterator}.
     */
    public static <T> Stream<T> stream(final Iterator<T> iterator) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.IMMUTABLE | Spliterator.SIZED), false)
                .onClose(() -> CloseableIterator.closeIterator(iterator));
    }

    public static <T> Stream<T> stream(final Iterable<T> iterable) {
        return IteratorUtils.stream(iterable.iterator());
    }
}
