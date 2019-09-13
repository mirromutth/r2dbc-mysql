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

package dev.miku.r2dbc.mysql.collation;

import static dev.miku.r2dbc.mysql.internal.AssertUtils.require;

/**
 * Base class for {@link CharsetTarget}.
 */
abstract class AbstractCharsetTarget implements CharsetTarget {

    final int byteSize;

    AbstractCharsetTarget(int byteSize) {
        require(byteSize > 0, "byteSize must be a positive integer");

        this.byteSize = byteSize;
    }

    @Override
    public final int getByteSize() {
        return byteSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AbstractCharsetTarget)) {
            return false;
        }

        AbstractCharsetTarget that = (AbstractCharsetTarget) o;

        return byteSize == that.byteSize;
    }

    @Override
    public int hashCode() {
        return byteSize;
    }
}
