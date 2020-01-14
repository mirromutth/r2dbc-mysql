/*
 * Copyright 2018-2020 the original author or authors.
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
 * Constants for MySQL prepared statement cursor types.
 */
public final class CursorTypes {

    /**
     * Just return all elements without fetch.
     */
    public static final byte NO_CURSOR = 0;

    /**
     * Read only cursor, it will be closed when prepared statement closing.
     */
    public static final byte READ_ONLY = 1;

//    public static final byte FOR_UPDATE = 2;
//    public static final byte SCROLLABLE = 4;

    private CursorTypes() {
    }
}
