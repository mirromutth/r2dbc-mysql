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

import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Arrays;
import java.util.Objects;

import static dev.miku.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * Mixin {@link CharsetTarget} for select the optimal {@link Charset} in multiple {@link CharsetTarget}s.
 */
final class MixCharsetTarget extends AbstractCharsetTarget {

    @Nullable
    private final Charset fallbackCharset;

    private final CharsetTarget[] targets;

    MixCharsetTarget(int byteSize, CharsetTarget... targets) {
        this(byteSize, null, targets);
    }

    MixCharsetTarget(int byteSize, @Nullable Charset fallbackCharset, CharsetTarget... targets) {
        super(maxByteSize(requireNonNull(targets, "targets must not be null"), byteSize));

        this.fallbackCharset = fallbackCharset;
        this.targets = targets;
    }

    @Override
    public Charset getCharset() {
        if (fallbackCharset == null) {
            return getCharsetFallible();
        } else {
            return getCharsetNonFail(fallbackCharset);
        }
    }

    @Override
    public boolean isCached() {
        return false;
    }

    private Charset getCharsetFallible() {
        IllegalArgumentException err = null;

        for (CharsetTarget target : this.targets) {
            try {
                return target.getCharset();
            } catch (IllegalArgumentException e) {
                // UnsupportedCharsetException is subclass of IllegalArgumentException
                if (err == null) {
                    err = e;
                } else {
                    e.addSuppressed(err);
                    err = e;
                }
            }
        }

        if (err == null) {
            throw new UnsupportedCharsetException("Charset target not found in MixCharsetTarget");
        } else {
            throw err;
        }
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

        if (!Objects.equals(fallbackCharset, that.fallbackCharset)) {
            return false;
        }
        return Arrays.equals(targets, that.targets);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (fallbackCharset != null ? fallbackCharset.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(targets);
        return result;
    }

    @Override
    public String toString() {
        return "MixCharsetTarget{" +
            "fallbackCharset=" + fallbackCharset +
            ", targets=" + Arrays.toString(targets) +
            ", byteSize=" + byteSize +
            '}';
    }

    private Charset getCharsetNonFail(Charset fallback) {
        for (CharsetTarget target : this.targets) {
            try {
                return target.getCharset();
            } catch (IllegalArgumentException ignored) {
                // UnsupportedCharsetException is subclass of IllegalArgumentException
                // Charset not support, just ignore
            }
        }

        return fallback;
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
}
