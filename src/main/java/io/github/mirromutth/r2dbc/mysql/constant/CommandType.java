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
 * Command type for command phase which used to decoding state machine.
 */
public enum CommandType {

    STATEMENT_EXECUTE,
    STATEMENT_PREPARE,
    STATEMENT_SIMPLE,

    /**
     * Like ping, debug, init db, or some requests which response is only OK, Error,
     * or no response if it is NOT {@code ExchangeableMessage}.
     */
    UTILITIES_SIMPLE
}
