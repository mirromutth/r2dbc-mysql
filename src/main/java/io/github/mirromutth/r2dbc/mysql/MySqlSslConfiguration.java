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

package io.github.mirromutth.r2dbc.mysql;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import reactor.util.annotation.Nullable;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.net.URL;

import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * MySQL configuration of SSL.
 */
public final class MySqlSslConfiguration {

    private static final String DEFAULT_CERT_TYPE = "JKS";

    private static final ClientAuth DEFAULT_CLIENT_AUTH = ClientAuth.NONE;

    private final SslProvider sslProvider;

    private final ClientAuth clientAuth;

    private final String keyCertType;

    private final String trustCertType;

    @Nullable
    private final String[] tlsProtocols;

    @Nullable
    private final KeyManagerFactory keyManagerFactory;

    @Nullable
    private final URL keyCertUrl;

    @Nullable
    private final CharSequence keyCertPassword;

    @Nullable
    private final TrustManagerFactory trustManagerFactory;

    @Nullable
    private final URL trustCertUrl;

    @Nullable
    private final CharSequence trustCertPassword;

    private MySqlSslConfiguration(
        SslProvider sslProvider, ClientAuth clientAuth, @Nullable String[] tlsProtocols,
        @Nullable KeyManagerFactory keyManagerFactory, String keyCertType, @Nullable URL keyCertUrl, @Nullable CharSequence keyCertPassword,
        @Nullable TrustManagerFactory trustManagerFactory, String trustCertType, @Nullable URL trustCertUrl, @Nullable CharSequence trustCertPassword
    ) {
        this.sslProvider = requireNonNull(sslProvider, "sslProvider must not be null");
        this.clientAuth = requireNonNull(clientAuth, "clientAuth must not be null");
        this.tlsProtocols = tlsProtocols;
        this.keyManagerFactory = keyManagerFactory;
        this.keyCertType = requireNonNull(keyCertType, "keyCertType must not be null");
        this.keyCertUrl = keyCertUrl;
        this.keyCertPassword = keyCertPassword;
        this.trustManagerFactory = trustManagerFactory;
        this.trustCertType = requireNonNull(trustCertType, "trustCertType must not be null");
        this.trustCertUrl = trustCertUrl;
        this.trustCertPassword = trustCertPassword;
    }

    public SslProvider getSslProvider() {
        return sslProvider;
    }

    public ClientAuth getClientAuth() {
        return clientAuth;
    }

    public String getKeyCertType() {
        return keyCertType;
    }

    public String getTrustCertType() {
        return trustCertType;
    }

    @Nullable
    public String[] getTlsProtocols() {
        return tlsProtocols;
    }

    @Nullable
    public KeyManagerFactory getKeyManagerFactory() {
        return keyManagerFactory;
    }

    @Nullable
    public URL getKeyCertUrl() {
        return keyCertUrl;
    }

    @Nullable
    public CharSequence getKeyCertPassword() {
        return keyCertPassword;
    }

    @Nullable
    public TrustManagerFactory getTrustManagerFactory() {
        return trustManagerFactory;
    }

    @Nullable
    public URL getTrustCertUrl() {
        return trustCertUrl;
    }

    @Nullable
    public CharSequence getTrustCertPassword() {
        return trustCertPassword;
    }

    static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private SslProvider sslProvider = SslContext.defaultClientProvider();

        private ClientAuth clientAuth = DEFAULT_CLIENT_AUTH;

        private String keyCertType = DEFAULT_CERT_TYPE;

        private String trustCertType = DEFAULT_CERT_TYPE;

        @Nullable
        private String[] tlsProtocols;

        @Nullable
        private KeyManagerFactory keyManagerFactory;

        @Nullable
        private URL keyCertUrl;

        @Nullable
        private CharSequence keyCertPassword;

        @Nullable
        private TrustManagerFactory trustManagerFactory;

        @Nullable
        private URL trustCertUrl;

        @Nullable
        private CharSequence trustCertPassword;

        private Builder() {
        }

        public Builder sslProvider(SslProvider sslProvider) {
            this.sslProvider = requireNonNull(sslProvider, "sslProvider must not be null");
            return this;
        }

        public Builder clientAuth(ClientAuth clientAuth) {
            this.clientAuth = requireNonNull(clientAuth, "clientAuth must not be null");
            return this;
        }

        public Builder disableServerVerify() {
            return trustManagerFactory(InsecureTrustManagerFactory.INSTANCE);
        }

        public Builder tlsProtocols(@Nullable String[] tlsProtocols) {
            this.tlsProtocols = tlsProtocols;
            return this;
        }

        public Builder keyCertType(String keyCertType) {
            this.keyCertType = requireNonNull(keyCertType, "keyCertType must not be null");
            return this;
        }

        public Builder keyCertUrl(URL keyCertUrl, @Nullable CharSequence keyCertPassword) {
            this.keyCertUrl = keyCertUrl;
            this.keyCertPassword = keyCertPassword;
            this.keyManagerFactory = null;
            return this;
        }

        public Builder keyManagerFactory(KeyManagerFactory keyManagerFactory) {
            this.keyManagerFactory = keyManagerFactory;
            this.keyCertUrl = null;
            this.keyCertPassword = null;
            return this;
        }

        public Builder trustCertType(String trustCertType) {
            this.trustCertType = requireNonNull(trustCertType, "trustCertType must not be null");
            return this;
        }

        public Builder trustCertUrl(URL trustCertUrl, @Nullable CharSequence trustCertPassword) {
            this.trustCertUrl = trustCertUrl;
            this.trustCertPassword = trustCertPassword;
            this.trustManagerFactory = null;
            return this;
        }

        public Builder trustManagerFactory(TrustManagerFactory trustManagerFactory) {
            this.trustManagerFactory = trustManagerFactory;
            this.trustCertUrl = null;
            this.trustCertPassword = null;
            return this;
        }

        MySqlSslConfiguration build() {
            return new MySqlSslConfiguration(
                sslProvider, clientAuth, tlsProtocols,
                keyManagerFactory, keyCertType, keyCertUrl, keyCertPassword,
                trustManagerFactory, trustCertType, trustCertUrl, trustCertPassword
            );
        }
    }
}
