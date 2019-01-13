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

package io.github.mirromutth.r2dbc.mysql.constant;

/**
 * Global constants for MySQL protocol, do NOT use it outer than {@code r2dbc-mysql},
 * we can NOT promise it will never be changes.
 */
public final class ProtocolConstants {

    private ProtocolConstants() {
    }

    /**
     * The length of the byte size field, it is 3 bytes.
     */
    public static final int SIZE_FIELD_SIZE = 3;

    public static final int ENVELOPE_HEADER_SIZE = SIZE_FIELD_SIZE + 1;

    public static final int MAX_PART_SIZE = ~(-(1 << (SIZE_FIELD_SIZE << 3)));

    public static final byte TERMINAL = 0;
}
