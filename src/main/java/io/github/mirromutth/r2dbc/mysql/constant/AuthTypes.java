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
 * MySQL Plugin type for handshake authentication
 * <p>
 * Connect a MySQL server those test database, execute
 * `SELECT * FROM `information_schema`.`PLUGINS` where `plugin_type` = 'AUTHENTICATION';`
 * could see what authentication plugin types those this MySQL server supports.
 * <p>
 * Old Password Authentication will NEVER support, the hashing algorithm has broken that
 * is used for this authentication type (as shown in CVE-2000-0981).
 */
public final class AuthTypes {

    private AuthTypes() {
    }

    public static final String MYSQL_NATIVE_PASSWORD = "mysql_native_password";

    public static final String SHA256_PASSWORD = "sha256_password";

    public static final String CACHING_SHA2_PASSWORD = "caching_sha2_password";
}
