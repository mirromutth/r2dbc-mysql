/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.miku.r2dbc.mysql;

import dev.miku.r2dbc.mysql.util.InternalArrays;

import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * An implementation of {@link Set<String>} for {@code RowMetadata.getColumnNames} results.
 *
 * @see MySqlNames column name searching rules.
 */
final class ColumnNameSet extends AbstractSet<String> implements Set<String> {

    private final String[] names;

    /**
     * Construct a {@link ColumnNameSet} by sorted {@code names} without array copy.
     *
     * @param names must be sorted by {@link MySqlNames#compare(String, String)}.
     */
    ColumnNameSet(String... names) {
        this.names = names;
    }

    @Override
    public boolean contains(Object o) {
        if (o instanceof String) {
            return MySqlNames.nameSearch(this.names, (String) o) >= 0;
        }

        return false;
    }

    @Override
    public Iterator<String> iterator() {
        return InternalArrays.asIterator(names);
    }

    @Override
    public int size() {
        return names.length;
    }

    @Override
    public boolean isEmpty() {
        return names.length == 0;
    }

    @Override
    public Spliterator<String> spliterator() {
        return Spliterators.spliterator(this.names, Spliterator.NONNULL | Spliterator.ORDERED | Spliterator.IMMUTABLE);
    }

    @Override
    public void forEach(Consumer<? super String> action) {
        Objects.requireNonNull(action);

        for (String name : this.names) {
            action.accept(name);
        }
    }

    @Override
    public String[] toArray() {
        return Arrays.copyOf(names, names.length);
    }

    @Override
    public <T> T[] toArray(T[] ts) {
        Objects.requireNonNull(ts);

        @SuppressWarnings("unchecked")
        T[] names = (T[]) this.names;

        if (ts.length < names.length) {
            return Arrays.copyOf(names, names.length);
        } else {
            System.arraycopy(names, 0, ts, 0, names.length);
            if (ts.length > names.length) {
                ts[names.length] = null;
            }
            return ts;
        }
    }

    @Override
    public boolean add(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends String> c) {
        Objects.requireNonNull(c);

        if (!c.isEmpty()) {
            throw new UnsupportedOperationException();
        }

        return false;
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeIf(Predicate<? super String> filter) {
        Objects.requireNonNull(filter);

        for (String name : this.names) {
            if (filter.test(name)) {
                throw new UnsupportedOperationException();
            }
        }

        return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        Objects.requireNonNull(c);

        if (!c.isEmpty()) {
            throw new UnsupportedOperationException();
        }

        return false;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        Objects.requireNonNull(c);

        if (!c.containsAll(this)) {
            throw new UnsupportedOperationException();
        }

        return false;
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return Arrays.toString(names);
    }
}
