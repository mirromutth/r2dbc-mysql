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

package io.github.mirromutth.r2dbc.mysql.collation;

import java.nio.charset.Charset;

import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * An implementation of {@link CharsetTarget} that has cached charset.
 */
final class CachedCharsetTarget extends AbstractCharsetTarget {

    private final Charset charset;

    CachedCharsetTarget(int byteSize, Charset charset) {
        super(byteSize);

        this.charset = requireNonNull(charset, "charset must not be null");
    }

    @Override
    public Charset getCharset() {
        return charset;
    }

    @Override
    public boolean isCached() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CachedCharsetTarget)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        CachedCharsetTarget that = (CachedCharsetTarget) o;

        return charset.equals(that.charset);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + charset.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "CachedCharsetTarget{" +
            "charset=" + charset +
            ", byteSize=" + byteSize +
            '}';
    }
}
