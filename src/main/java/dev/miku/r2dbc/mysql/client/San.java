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

package dev.miku.r2dbc.mysql.client;

import dev.miku.r2dbc.mysql.internal.AssertUtils;

import java.util.Objects;

/**
 * Subject Alternative Name (aka. SAN, subjectAltName) in SSL.
 * <p>
 * RFC 5280, Section 4.1.2.6
 * The subject name maybe carried in the subject field and/or the subjectAltName extension
 */
final class San {

    static final int DNS = 2;

    static final int IP = 7;

    private final String value;

    private final int type;

    San(String value, int type) {
        AssertUtils.require(type > 0, "type must be a positive integer");

        this.value = AssertUtils.requireNonNull(value, "value must not be null");
        this.type = type;
    }

    int getType() {
        return type;
    }

    String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof San)) {
            return false;
        }
        San that = (San) o;
        return type == that.type && value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, type);
    }

    @Override
    public String toString() {
        return value;
    }
}
