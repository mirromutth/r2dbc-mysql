/*
 * Copyright 2018-2020 the original author or authors.
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

import static dev.miku.r2dbc.mysql.util.AssertUtils.require;
import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link Set}{@code <}{@link String}{@code >} for {@code RowMetadata.getColumnNames}
 * results.
 *
 * @see MySqlNames column name searching rules.
 */
final class ColumnNameSet extends AbstractSet<String> implements Set<String> {

    private final String[] originNames;

    private final String[] sortedNames;

    /**
     * Construct a {@link ColumnNameSet} by sorted {@code names} without array copy.
     *
     * @param originNames must be the original order.
     * @param sortedNames must be sorted by {@link MySqlNames#compare(String, String)}.
     */
    private ColumnNameSet(String[] originNames, String[] sortedNames) {
        this.originNames = originNames;
        this.sortedNames = sortedNames;
    }

    @Override
    public boolean contains(Object o) {
        if (o instanceof String) {
            return MySqlNames.nameSearch(this.sortedNames, (String) o) >= 0;
        }

        return false;
    }

    @Override
    public Iterator<String> iterator() {
        return InternalArrays.asIterator(originNames);
    }

    @Override
    public int size() {
        return originNames.length;
    }

    @Override
    public boolean isEmpty() {
        return originNames.length == 0;
    }

    @Override
    public Spliterator<String> spliterator() {
        return Spliterators.spliterator(this.originNames,
            Spliterator.NONNULL | Spliterator.ORDERED | Spliterator.IMMUTABLE);
    }

    @Override
    public void forEach(Consumer<? super String> action) {
        Objects.requireNonNull(action);

        for (String name : this.originNames) {
            action.accept(name);
        }
    }

    @Override
    public String[] toArray() {
        return Arrays.copyOf(originNames, originNames.length);
    }

    @SuppressWarnings({ "unchecked", "SuspiciousSystemArraycopy" })
    @Override
    public <T> T[] toArray(T[] a) {
        Objects.requireNonNull(a);

        int size = originNames.length;

        if (a.length < size) {
            return (T[]) Arrays.copyOf(originNames, size, a.getClass());
        } else {
            System.arraycopy(originNames, 0, a, 0, size);

            if (a.length > size) {
                a[size] = null;
            }

            return a;
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

        for (String name : this.originNames) {
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
        return Arrays.toString(originNames);
    }

    static ColumnNameSet of(String name) {
        requireNonNull(name, "name must not be null");

        String[] names = new String[] { name };
        return new ColumnNameSet(names, names);
    }

    static ColumnNameSet of(String[] originNames, String[] sortedNames) {
        requireNonNull(originNames, "originNames must not be null");
        requireNonNull(sortedNames, "sortedNames must not be null");
        require(originNames.length == sortedNames.length,
            "The length of origin names the same as sorted names one");

        return new ColumnNameSet(originNames, sortedNames);
    }
}
