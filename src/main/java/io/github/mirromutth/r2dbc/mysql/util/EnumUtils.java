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

package io.github.mirromutth.r2dbc.mysql.util;

import io.github.mirromutth.r2dbc.mysql.constant.AuthType;
import io.github.mirromutth.r2dbc.mysql.constant.ProtocolVersion;
import io.github.mirromutth.r2dbc.mysql.exception.AuthTypeNotSupportException;
import io.github.mirromutth.r2dbc.mysql.exception.ProtocolNotSupportException;

/**
 * The utility for convert native code/flag/name to enumerable class.
 */
public final class EnumUtils {

    private EnumUtils() {
    }

    public static ProtocolVersion protocolVersion(int code) {
        if (code != ProtocolVersion.V10.getCode()) {
            throw new ProtocolNotSupportException(code);
        }

        return ProtocolVersion.V10;
    }

    public static AuthType authType(String nativeName) {
        try {
            return AuthType.valueOf(nativeName.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new AuthTypeNotSupportException(nativeName, e);
        }
    }
}
