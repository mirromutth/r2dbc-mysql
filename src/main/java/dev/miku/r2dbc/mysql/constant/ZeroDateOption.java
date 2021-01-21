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
 * MySQL server maybe return "0000-00-00 00:00:00" (DATETIME, TIMESTAMP, etc.)
 * or "0000-00-00" (DATE), call them "zero date".
 * <p>
 * This option indicates special handling when MySQL server returning "zero date".
 */
public enum ZeroDateOption {

    /**
     * Just throw a exception when MySQL server return "zero date".
     * <p>
     * Now will throws {@code R2dbcNonTransientResourceException}.
     */
    EXCEPTION,

    /**
     * Use {@code null} when MySQL server return "zero date".
     */
    USE_NULL,

    /**
     * Use "round" date (i.e. "0001-01-01 00:00:00" or "0001-01-01")
     * when MySQL server return "zero date".
     * <p>
     * This option is only for compatibility with certain code and NOT recommended.
     */
    USE_ROUND
}
