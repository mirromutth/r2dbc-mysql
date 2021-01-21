/*
 * Copyright 2018-2021 the original author or authors.
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

package dev.miku.r2dbc.mysql.constant;

/**
 * Constants for MySQL protocol envelopes (e.g. business layer packages).
 * <p>
 * WARNING: do NOT use it outer than {@literal r2dbc-mysql}.
 */
public final class Envelopes {

    /**
     * The length of the byte size field, it is 3 bytes.
     */
    public static final int SIZE_FIELD_SIZE = 3;

    /**
     * The byte size of header part.
     */
    public static final int PART_HEADER_SIZE = SIZE_FIELD_SIZE + 1;

    /**
     * The max bytes size of each envelope, value is 16777215. (i.e. max value of int24, (2 ** 24) - 1)
     */
    public static final int MAX_ENVELOPE_SIZE = (1 << (SIZE_FIELD_SIZE << 3)) - 1;

    /**
     * The terminal of C-style string or C-style binary data.
     */
    public static final byte TERMINAL = 0;

    private Envelopes() { }
}
