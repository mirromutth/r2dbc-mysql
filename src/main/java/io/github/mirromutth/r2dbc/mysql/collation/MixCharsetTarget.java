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

import io.github.mirromutth.r2dbc.mysql.core.ServerVersion;

import java.nio.charset.Charset;
import java.util.Arrays;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * Mixin {@link CharsetTarget} for select the optimal {@link Charset} in multiple {@link CharsetTarget}s.
 */
final class MixCharsetTarget extends AbstractCharsetTarget {

    private final Charset fallbackCharset;

    private final CharsetTarget[] targets;

    MixCharsetTarget(int byteSize, Charset fallbackCharset, CharsetTarget... targets) {
        this(byteSize, ServerVersion.NONE, fallbackCharset, targets);
    }

    private MixCharsetTarget(int byteSize, ServerVersion minVersion, Charset fallbackCharset, CharsetTarget... targets) {
        super(maxByteSize(requireNonNull(targets, "targets must not be null"), byteSize), minVersion);

        this.fallbackCharset = requireNonNull(fallbackCharset, "fallbackCharset must not be null");
        this.targets = targets;
    }

    @Override
    public Charset getCharset() {
        for (CharsetTarget target : targets) {
            try {
                return target.getCharset();
            } catch (IllegalArgumentException ignored) {
                // Charset not support, just ignore
            }
        }

        return fallbackCharset;
    }

    @Override
    public boolean isCached() {
        return false;
    }

    private static int maxByteSize(CharsetTarget[] targets, int defaultByteSize) {
        int result = defaultByteSize;

        for (CharsetTarget target : targets) {
            int byteSize = target.getByteSize();
            if (byteSize > result) {
                result = byteSize;
            }
        }

        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MixCharsetTarget)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        MixCharsetTarget that = (MixCharsetTarget) o;

        if (!fallbackCharset.equals(that.fallbackCharset)) {
            return false;
        }
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        return Arrays.equals(targets, that.targets);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + fallbackCharset.hashCode();
        result = 31 * result + Arrays.hashCode(targets);
        return result;
    }

    @Override
    public String toString() {
        return "MixCharsetTarget{" +
            "fallbackCharset=" + fallbackCharset +
            ", targets=" + Arrays.toString(targets) +
            ", byteSize=" + byteSize +
            ", minVersion=" + minVersion +
            '}';
    }
}
