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
 * All TLS protocol names supported by MySQL.
 */
public final class TlsProtocols {

    private TlsProtocols() {
    }

    public static final String TLS1 = "TLSv1";

    public static final String TLS1_1 = "TLSv1.1";

    /**
     * Supported only in Community Edition {@literal 8.0.4+} or Enterprise Edition {@literal 5.6.0+}.
     * The {@literal '+'} means that this version or higher is included.
     * <p>
     * Note: The Enterprise Edition version will looks like {@literal 5.7.26-enterprise}.
     */
    public static final String TLS1_2 = "TLSv1.2";
}
