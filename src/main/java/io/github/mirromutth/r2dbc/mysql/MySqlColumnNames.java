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

package io.github.mirromutth.r2dbc.mysql;

/**
 * A utility considers column names searching logic which use a special compare rule
 * for sort and special binary search.
 *
 * <ul>
 * <li>Sort: compare with case insensitive first, then compare with case sensitive
 * when they equals by case insensitive.</li>
 * <li>Search: find with case sensitive first, then find with case insensitive
 * when not found in case sensitive.</li>
 * </ul>
 *
 * <p>
 * For example:
 * Sort first: abc AB a Abc Ab ABC A ab b B -> A a B b AB Ab ab ABC Abc abc
 * Then find "aB" use the same compare rule,
 *
 * @see #compare(CharSequence, CharSequence)
 */
final class MySqlColumnNames {

    private MySqlColumnNames() {
    }

    /**
     * @param names column name
     * @param name  least 1 character enclosed by {@literal `} means it use case
     *              sensitive mode, otherwise use default mode (find with case
     *              sensitive first, then find with case insensitive when not
     *              found in case sensitive)
     * @return found index by special binary search, {@code -1} means not found.
     */
    static int nameSearch(CharSequence[] names, String name) {
        int size = name.length();
        if (size > 2 && name.charAt(0) == '`' && name.charAt(size - 1) == '`') {
            return binarySearch(names, slice(name, 1, size - 1), false);
        } else {
            return binarySearch(names, name, true);
        }
    }

    private static int binarySearch(CharSequence[] names, CharSequence name, boolean ignoreCase) {
        int left = 0;
        int right = names.length - 1;
        int ciResult = -1;

        while (left <= right) {
            // `left + (right - left) / 2` for ensure no overflow,
            // `left + (right - left) / 2` = `(left + right) >>> 1`
            // when `left` and `right` is not negative integer.
            // And `left` must greater or equals than 0,
            // `right` greater or equals than `left`.
            int middle = (left + right) >>> 1;
            CharSequence value = names[middle];
            int compared = compare(value, name);

            if (compared < 0) {
                left = middle + 1;

                if (compared == -2) {
                    ciResult = middle;
                }
            } else if (compared > 0) {
                right = middle - 1;

                if (compared == 2) {
                    ciResult = middle;
                }
            } else {
                // Case sensitive match, just return.
                return middle;
            }
        }

        if (ignoreCase && ciResult >= 0) {
            return ciResult;
        }

        return -1;
    }

    /**
     * A special compare rule {@code left} and {@code right}
     * <p>
     * Note: visible for unit tests.
     *
     * @param left  the {@link CharSequence} of left
     * @param right the {@link CharSequence} of right
     * @return {@code 0} means both strings equals even case sensitive,
     * absolute value is {@code 2} means it is equals by case insensitive but not equals when case sensitive,
     * absolute value is {@code 4} means it is not equals even case insensitive.
     */
    static int compare(CharSequence left, CharSequence right) {
        int leftSize = left.length();
        int rightSize = right.length();
        int minSize = leftSize < rightSize ? leftSize : rightSize;
        // Case sensitive comparator result.
        int csCompared = 0;

        for (int i = 0; i < minSize; i++) {
            char leftChar = left.charAt(i);
            char rightChar = right.charAt(i);

            if (leftChar != rightChar) {
                if (csCompared == 0) {
                    // Compare end if is case sensitive comparator.
                    csCompared = leftChar - rightChar;
                }

                // Use `Character.toLowerCase` for latin alpha.
                leftChar = Character.toLowerCase(leftChar);
                rightChar = Character.toLowerCase(rightChar);

                if (leftChar != rightChar) {
                    // Not equals even case insensitive.
                    return leftChar < rightChar ? -4 : 4;
                }
            }
        }

        // Length not equals means both strings not equals even case insensitive.
        if (leftSize != rightSize) {
            return leftSize < rightSize ? -4 : 4;
        } else {
            // Equals when case insensitive, use case sensitive.
            return csCompared < 0 ? -2 : (csCompared > 0 ? 2 : 0);
        }
    }

    /**
     * Slice {@link String} for no copy with immutable.
     * <p>
     * WARNING: visible for unit tests, do NOT use it outer than {@link MySqlColumnNames}.
     */
    static CharSequence slice(String content, int start, int end) {
        if (start < 0 || start > end || end > content.length()) {
            throw new StringIndexOutOfBoundsException(String.format("Start index %d and end index %d out of range", start, end));
        }

        if (start == end) {
            return Sliced.EMPTY;
        } else {
            return new Sliced(content, start, end);
        }
    }

    private static final class Sliced implements CharSequence {

        private static final Sliced EMPTY = new Sliced("", 0, 0);

        private final String content;

        private final int start;

        private final int end;

        private final int size;

        private Sliced(String content, int start, int end) {
            this.content = content;
            this.start = start;
            this.end = end;
            this.size = end - start;
        }

        @Override
        public int length() {
            return size;
        }

        @Override
        public char charAt(int index) {
            if (index < 0 || index >= size) {
                throw new StringIndexOutOfBoundsException(index);
            }

            return content.charAt(index + start);
        }

        @Override
        public Sliced subSequence(int start, int end) {
            if (start < 0 || start > end) {
                throw new StringIndexOutOfBoundsException(start);
            } else if (end > size) {
                throw new StringIndexOutOfBoundsException(end);
            }

            if (start == end) {
                return EMPTY;
            } else {
                return new Sliced(this.content, start + this.start, end + this.start);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Sliced)) {
                return false;
            }

            Sliced sliced = (Sliced) o;

            if (size != sliced.size) {
                return false;
            }

            for (int i = 0; i < size; ++i) {
                if (content.charAt(i + start) != sliced.charAt(i + sliced.start)) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = 0;

            for (int i = 0; i < size; ++i) {
                result = 31 * result + content.charAt(i + start);
            }

            return result;
        }

        @Override
        public String toString() {
            return content.substring(start, end);
        }
    }
}
