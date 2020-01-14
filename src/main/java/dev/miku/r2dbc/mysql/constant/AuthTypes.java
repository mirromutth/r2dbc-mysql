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
 * All authentication types from handshake requests.
 */
public final class AuthTypes {

    public static final String CACHING_SHA2_PASSWORD = "caching_sha2_password";

    public static final String MYSQL_NATIVE_PASSWORD = "mysql_native_password";

    public static final String SHA256_PASSWORD = "sha256_password";

    public static final String MYSQL_OLD_PASSWORD = "mysql_old_password";

    /**
     * Try use empty string to represent has no authentication provider
     * when {@code Capabilities.PLUGIN_AUTH} does not set.
     */
    public static final String NO_AUTH_PROVIDER = "";
}
