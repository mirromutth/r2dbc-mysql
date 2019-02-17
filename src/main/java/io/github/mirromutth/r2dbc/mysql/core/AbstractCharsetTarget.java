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

package io.github.mirromutth.r2dbc.mysql.core;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;
import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requirePositive;

/**
 * Generic {@link CharsetTarget} logic and properties.
 */
abstract class AbstractCharsetTarget implements CharsetTarget {

    final int byteSize;

    final ServerVersion minVersion;

    AbstractCharsetTarget(int byteSize, ServerVersion minVersion) {
        this.byteSize = requirePositive(byteSize, "byteSize must be positive integer");
        this.minVersion = requireNonNull(minVersion, "minVersion must not be null");
    }

    @Override
    public final int getByteSize() {
        return byteSize;
    }

    @Override
    public final boolean isExists(ServerVersion version) {
        return minVersion.compareTo(version) <= 0;
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

        if (byteSize != that.byteSize) {
            return false;
        }
        return minVersion.equals(that.minVersion);
    }

    @Override
    public int hashCode() {
        int result = byteSize;
        result = 31 * result + minVersion.hashCode();
        return result;
    }
}
