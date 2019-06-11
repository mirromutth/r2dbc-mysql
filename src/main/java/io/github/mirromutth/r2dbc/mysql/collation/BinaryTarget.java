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
import java.nio.charset.UnsupportedCharsetException;

/**
 * A special implementation of {@link AbstractCharsetTarget} which considers LOB field is BLOB field,
 * can not convert to CLOB.
 */
final class BinaryTarget extends AbstractCharsetTarget {

    static final BinaryTarget INSTANCE = new BinaryTarget();

    private BinaryTarget() {
        super(1);
    }

    @Override
    public Charset getCharset() throws UnsupportedCharsetException {
        throw new UnsupportedCharsetException("binary character collation has no charset");
    }

    @Override
    public boolean isCached() {
        return true;
    }
}
