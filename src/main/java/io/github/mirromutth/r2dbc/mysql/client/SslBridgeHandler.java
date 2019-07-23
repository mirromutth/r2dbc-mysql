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

package io.github.mirromutth.r2dbc.mysql.client;

import io.github.mirromutth.r2dbc.mysql.MySqlSslConfiguration;
import io.github.mirromutth.r2dbc.mysql.ServerVersion;
import io.github.mirromutth.r2dbc.mysql.constant.TlsProtocols;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.message.server.SyntheticSslResponseMessage;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.netty.tcp.SslProvider;
import reactor.util.annotation.Nullable;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * A handler for build SSL handler and bridging.
 */
final class SslBridgeHandler extends ChannelDuplexHandler {

    static final String NAME = "R2dbcMySqlSslBridgeHandler";

    private static final String SSL_NAME = "R2dbcMySqlSslHandler";

    private static final Logger logger = LoggerFactory.getLogger(SslBridgeHandler.class);

    /**
     * Lowest version of community edition for TLSv1.2 support.
     */
    private static final ServerVersion TLS1_2_COMMUNITY_VER = ServerVersion.create(8, 0, 4);

    /**
     * Lowest version of enterprise edition for TLSv1.2 support. Should be
     * judged in conjunction with {@link ServerVersion#isEnterprise()}.
     */
    private static final ServerVersion TLS1_2_ENTERPRISE_VER = ServerVersion.create(5, 6, 0);

    private final MySqlSession session;

    private volatile MySqlSslConfiguration sslConfiguration;

    SslBridgeHandler(MySqlSession session, MySqlSslConfiguration sslConfiguration) {
        this.session = requireNonNull(session, "session must not be null");
        this.sslConfiguration = requireNonNull(sslConfiguration, "sslConfiguration must not be null");
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof SslState) {
            switch ((SslState) evt) {
                case BRIDGING:
                    logger.debug("SSL event triggered, enable SSL handler to pipeline");

                    MySqlSslConfiguration sslConfiguration = this.sslConfiguration;
                    this.sslConfiguration = null;

                    if (sslConfiguration == null) {
                        ctx.fireExceptionCaught(new IllegalStateException("The SSL bridge has used, cannot build SSL handler twice"));
                        return;
                    }

                    SslProvider sslProvider = buildProvider(sslConfiguration, session.getServerVersion());
                    ctx.pipeline().addBefore(NAME, SSL_NAME, sslProvider.getSslContext().newHandler(ctx.alloc()));
                    break;
                case UNSUPPORTED:
                    // Remove self because it is useless. (kick down the ladder!)
                    logger.debug("Server unsupported SSL, remove SSL bridge in pipeline");
                    ctx.pipeline().remove(NAME);
                    break;
            }
            // Ignore custom SSL state because it is useless.
        } else if (SslHandshakeCompletionEvent.SUCCESS == evt) {
            ctx.fireChannelRead(SyntheticSslResponseMessage.getInstance());
            super.userEventTriggered(ctx, evt);

            // Remove self because it is useless. (kick down the ladder!)
            logger.debug("SSL handshake completed, remove SSL bridge in pipeline");
            ctx.pipeline().remove(NAME);
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }

    private static SslProvider buildProvider(MySqlSslConfiguration sslConfiguration, ServerVersion version) {
        return SslProvider.builder()
            .sslContext(buildContext(sslConfiguration, version))
            .defaultConfiguration(SslProvider.DefaultConfigurationType.TCP)
            .build();
    }

    private static SslContextBuilder buildContext(MySqlSslConfiguration sslConfiguration, ServerVersion version) {
        SslContextBuilder builder = withTlsProtocols(SslContextBuilder.forClient(), sslConfiguration, version)
            .sslProvider(sslConfiguration.getSslProvider())
            .clientAuth(sslConfiguration.getClientAuth());

        KeyManagerFactory keyManagerFactory = buildKeyManager(
            sslConfiguration.getKeyManagerFactory(),
            sslConfiguration.getKeyCertType(),
            sslConfiguration.getKeyCertUrl(),
            sslConfiguration.getKeyCertPassword()
        );

        if (keyManagerFactory != null) {
            builder.keyManager(keyManagerFactory);
        }

        TrustManagerFactory trustManagerFactory = buildTrustManager(
            sslConfiguration.getTrustManagerFactory(),
            sslConfiguration.getTrustCertType(),
            sslConfiguration.getTrustCertUrl(),
            sslConfiguration.getTrustCertPassword()
        );

        if (trustManagerFactory != null) {
            builder.trustManager(trustManagerFactory);
        }

        return builder;
    }

    private static SslContextBuilder withTlsProtocols(SslContextBuilder builder, MySqlSslConfiguration sslConfiguration, ServerVersion version) {
        String[] tlsProtocols = sslConfiguration.getTlsProtocols();

        if (tlsProtocols.length > 0) {
            builder.protocols(tlsProtocols);
        } else if (isEnabledTls1_2(version)) {
            builder.protocols(TlsProtocols.TLS1, TlsProtocols.TLS1_1, TlsProtocols.TLS1_2);
        } else {
            builder.protocols(TlsProtocols.TLS1, TlsProtocols.TLS1_1);
        }

        return builder;
    }

    @Nullable
    private static KeyManagerFactory buildKeyManager(
        @Nullable KeyManagerFactory managerFactory,
        String keyCertType, @Nullable URL keyCertUrl, @Nullable CharSequence keyCertPassword
    ) {
        if (managerFactory != null) {
            return managerFactory;
        } else if (keyCertUrl == null) {
            return null;
        }

        try {
            char[] password = convertPlainPassword(keyCertPassword);
            KeyStore keyStore = buildKeyStore(keyCertType, keyCertUrl, password);
            KeyManagerFactory factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());

            factory.init(keyStore, password);
            return factory;
        } catch (KeyStoreException | IOException | CertificateException | NoSuchAlgorithmException | UnrecoverableKeyException e) {
            throw new IllegalStateException("Load key cert (client-side) failed!", e);
        }
    }

    @Nullable
    private static TrustManagerFactory buildTrustManager(
        @Nullable TrustManagerFactory managerFactory,
        String trustCertType, @Nullable URL trustCertUrl, @Nullable CharSequence trustCertPassword
    ) {
        if (managerFactory != null) {
            return managerFactory;
        } else if (trustCertUrl == null) {
            return null;
        }

        try {
            char[] password = convertPlainPassword(trustCertPassword);
            KeyStore keyStore = buildKeyStore(trustCertType, trustCertUrl, password);
            TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

            factory.init(keyStore);
            return factory;
        } catch (KeyStoreException | IOException | CertificateException | NoSuchAlgorithmException e) {
            throw new IllegalStateException("Load trust cert (server-side) failed!", e);
        }
    }

    private static KeyStore buildKeyStore(String certType, URL certUrl, @Nullable char[] password)
        throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException {
        KeyStore keyStore = KeyStore.getInstance(certType);

        try (InputStream inputStream = certUrl.openStream()) {
            keyStore.load(inputStream, password);
            return keyStore;
        }
    }

    @Nullable
    private static char[] convertPlainPassword(@Nullable CharSequence password) {
        if (password == null) {
            return null;
        } else {
            int length = password.length();
            char[] result = new char[length];

            for (int i = 0; i < length; ++i) {
                result[i] = password.charAt(i);
            }

            return result;
        }
    }

    private static boolean isEnabledTls1_2(ServerVersion version) {
        return version.isGreaterThanOrEqualTo(TLS1_2_COMMUNITY_VER) || (version.isGreaterThanOrEqualTo(TLS1_2_ENTERPRISE_VER) && version.isEnterprise());
    }
}
