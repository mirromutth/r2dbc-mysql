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
import io.github.mirromutth.r2dbc.mysql.plugin.AuthPlugin;
import io.github.mirromutth.r2dbc.mysql.plugin.CachingSha2PasswordAuthPlugin;
import io.github.mirromutth.r2dbc.mysql.plugin.NativePasswordAuthPlugin;
import io.github.mirromutth.r2dbc.mysql.plugin.Sha256PasswordAuthPlugin;

import static java.util.Objects.requireNonNull;

/**
 * MySQL Plugins Utils, like Storage Engine, Authentication, Audit, Information Schema, FtParser, Daemon, etc.
 */
public class PluginUtils {

    public static AuthPlugin getAuthPlugin(AuthType type) {
        switch (requireNonNull(type)) {
            case MYSQL_NATIVE_PASSWORD:
                return NativePasswordAuthPlugin.getInstance();
            case SHA256_PASSWORD:
                return Sha256PasswordAuthPlugin.getInstance();
            case CACHING_SHA2_PASSWORD:
                return CachingSha2PasswordAuthPlugin.getInstance();
        }

        throw new IllegalArgumentException("Unsupported authentication plugin type " + type);
    }
}
