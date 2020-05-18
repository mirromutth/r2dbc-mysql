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

package dev.miku.r2dbc.mysql.util;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An utility with constants of arrays for reduce temporary objects and
 * ensure immutability, do NOT use it outer than {@literal r2dbc-mysql}.
 */
public final class InternalArrays {

    public static final byte[] EMPTY_BYTES = {};

    public static final String[] EMPTY_STRINGS = {};

    /**
     * Wrap an array to an immutable {@link List} without deep copy.
     * <p>
     * WARNING: make sure {@code a} will not be changed by other one.
     *
     * @param a   the array which want to be wrapped.
     * @param <E> the type for elements of {@code a}.
     * @return read only {@link List}.
     */
    @SuppressWarnings("varargs")
    @SafeVarargs
    public static <E> List<E> asReadOnlyList(E... a) {
        requireNonNull(a, "array must not be null");

        switch (a.length) {
            case 0:
                return Collections.emptyList();
            case 1:
                return Collections.singletonList(a[0]);
            default:
                return new ArrList<>(a);
        }
    }

    /**
     * Convert an array to a read only {@link List} with array copy.
     * It will copy array but will not clone each element.
     *
     * @param a   the array which want to be converted.
     * @param <E> the type for elements of {@code a}.
     * @return read only {@link List}.
     */
    @SuppressWarnings("varargs")
    @SafeVarargs
    public static <E> List<E> toReadOnlyList(E... a) {
        requireNonNull(a, "array must not be null");

        switch (a.length) {
            case 0:
                return Collections.emptyList();
            case 1:
                return Collections.singletonList(a[0]);
            default:
                return new ArrList<>(Arrays.copyOf(a, a.length));
        }
    }

    /**
     * Wrap an array to a read only {@link Iterator}.
     * <p>
     * WARNING: make sure {@code a} will not be changed by other one.
     *
     * @param a   the array which want to be wrapped.
     * @param <E> the type for elements of {@code a}.
     * @return read only {@link Iterator}.
     */
    @SuppressWarnings("varargs")
    @SafeVarargs
    public static <E> Iterator<E> asIterator(E... a) {
        requireNonNull(a, "array must not be null");

        if (a.length == 0) {
            return Collections.emptyIterator();
        }

        return new ArrItr<>(0, a);
    }

    private InternalArrays() {
    }
}

final class ArrItr<E> implements ListIterator<E>, Iterator<E> {

    private int i;

    private final E[] a;

    ArrItr(int i, E[] a) {
        this.i = i;
        this.a = a;
    }

    @Override
    public boolean hasNext() {
        return i < a.length;
    }

    @Override
    public E next() {
        if (i >= a.length) {
            throw new NoSuchElementException();
        }

        return a[i++];
    }

    @Override
    public boolean hasPrevious() {
        return i > 0;
    }

    @Override
    public E previous() {
        if (i <= 0) {
            throw new NoSuchElementException();
        }

        return a[--i];
    }

    public int nextIndex() {
        return i;
    }

    public int previousIndex() {
        return i - 1;
    }

    @Override
    public void forEachRemaining(Consumer<? super E> action) {
        Objects.requireNonNull(action);

        while (i < a.length) {
            action.accept(a[i++]);
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void set(E e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void add(E e) {
        throw new UnsupportedOperationException();
    }
}

final class ArrList<E> extends AbstractList<E> implements List<E>, RandomAccess {

    private final E[] a;

    ArrList(E[] a) {
        this.a = a;
    }

    @Override
    public E get(int index) {
        return a[index];
    }

    @Override
    public int size() {
        return a.length;
    }

    @Override
    public boolean isEmpty() {
        return a.length == 0;
    }

    @Override
    public Iterator<E> iterator() {
        return new ArrItr<>(0, a);
    }

    @Override
    public ListIterator<E> listIterator(int index) {
        if (index < 0 || index > a.length) {
            throw new IndexOutOfBoundsException();
        }

        return new ArrItr<>(index, a);
    }

    @Override
    public Spliterator<E> spliterator() {
        return Arrays.spliterator(a);
    }

    @Override
    public void forEach(Consumer<? super E> action) {
        Objects.requireNonNull(action);
        for (E t : a) {
            action.accept(t);
        }
    }

    @Override
    public Object[] toArray() {
        return Arrays.copyOf(a, a.length);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T[] toArray(T[] a) {
        T[] source = (T[]) this.a;

        if (a.length < source.length) {
            // Make a new array of a's runtime type, but my contents:
            return (T[]) Arrays.copyOf(source, source.length, a.getClass());
        }

        System.arraycopy(source, 0, a, 0, this.a.length);

        if (a.length > source.length) {
            a[source.length] = null;
        }

        return a;
    }

    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeIf(Predicate<? super E> filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void replaceAll(UnaryOperator<E> operator) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sort(Comparator<? super E> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }
}
