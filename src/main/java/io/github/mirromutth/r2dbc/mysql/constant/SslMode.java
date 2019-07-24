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
 * The mode of SSL considers the security of MySQL connections.
 */
public enum SslMode {

    /**
     * Establish an unencrypted connection.
     * <p>
     * In other words: I don't care about security and don't want to pay the overhead for encryption.
     */
    DISABLED,

    /**
     * Establish an encrypted connection if the server supports encrypted connections, otherwise fallback to an unencrypted connection.
     * <p>
     * In other words: I don't care about encryption but will pay the overhead of encryption if the server supports it.
     */
    PREFERRED,

    /**
     * Establish an encrypted connection if the server supports encrypted connections, otherwise the connection fails.
     * <p>
     * In other words: I want my data to be encrypted, and I accept the overhead. I trust that the network will make sure I always connect to the server I want.
     */
    REQUIRED,

    /**
     * Establish an encrypted connection based on {@link #REQUIRED} with verify the server Certificate Authority (CA) certificates.
     * The connection attempt fails if CA certificates mismatched.
     * <p>
     * In other words: I want my data encrypted, and I accept the overhead. I want to be sure that I connect to a server that I trust.
     */
    VERIFY_CA,

    /**
     * Establish an encrypted connection based on {@link #VERIFY_CA}, additionally perform identity verification by checking the hostname.
     * The connection attempt fails if the identity mismatched.
     * <p>
     * In other words: I want my data encrypted, and I accept the overhead. I want to be sure that I connect to a server I trust, and that it's the one I specify.
     */
    VERIFY_IDENTITY;

    public final boolean requireSsl() {
        return REQUIRED == this || VERIFY_CA == this || VERIFY_IDENTITY == this;
    }

    public final boolean startSsl() {
        return DISABLED != this;
    }

    public final boolean verifyCertificate() {
        return VERIFY_CA == this || VERIFY_IDENTITY == this;
    }

    public final boolean verifyIdentity() {
        return VERIFY_IDENTITY == this;
    }
}
