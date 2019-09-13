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

package dev.miku.r2dbc.mysql.constant;

/**
 * Constants of date/time in binary result/parameter.
 */
public final class BinaryDateTimes {

    public static final int DATE_SIZE = Short.BYTES + (Byte.BYTES << 1);

    public static final int DATETIME_SIZE = DATE_SIZE + (Byte.BYTES * 3);

    public static final int MICRO_DATETIME_SIZE = DATETIME_SIZE + Integer.BYTES;

    public static final int TIME_SIZE = Byte.BYTES + Integer.BYTES + (Byte.BYTES * 3);

    public static final int MICRO_TIME_SIZE = TIME_SIZE + Integer.BYTES;

    private BinaryDateTimes() {
    }
}
