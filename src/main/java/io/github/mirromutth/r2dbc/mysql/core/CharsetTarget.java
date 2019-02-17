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

import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;

/**
 * MySQL character collation target of {@link Charset}.
 */
interface CharsetTarget {

    /**
     * @return The maximum number of bytes by a character
     */
    int getByteSize();

    /**
     * @return Target default charset
     * @throws UnsupportedCharsetException throw if default charset unsupported on this JVM
     */
    Charset getCharset() throws UnsupportedCharsetException;

    boolean isExists(ServerVersion version);
}
