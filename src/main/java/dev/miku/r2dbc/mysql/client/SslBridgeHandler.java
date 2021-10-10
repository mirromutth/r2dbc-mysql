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

package dev.miku.r2dbc.mysql.client;

import dev.miku.r2dbc.mysql.ConnectionContext;
import dev.miku.r2dbc.mysql.MySqlSslConfiguration;
import dev.miku.r2dbc.mysql.ServerVersion;
import dev.miku.r2dbc.mysql.constant.SslMode;
import dev.miku.r2dbc.mysql.constant.TlsVersions;
import dev.miku.r2dbc.mysql.message.server.SyntheticSslResponseMessage;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.IdentityCipherSuiteFilter;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import reactor.netty.tcp.SslProvider;
import reactor.util.Logger;
import reactor.util.Loggers;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import java.io.File;
import java.net.InetSocketAddress;
import java.util.function.Consumer;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;
import static io.netty.handler.ssl.SslProvider.JDK;
import static io.netty.handler.ssl.SslProvider.OPENSSL;

/**
 * A handler for build SSL handler and bridging.
 */
final class SslBridgeHandler extends ChannelDuplexHandler {

    static final String NAME = "R2dbcMySqlSslBridgeHandler";

    private static final String SSL_NAME = "R2dbcMySqlSslHandler";

    private static final Logger logger = Loggers.getLogger(SslBridgeHandler.class);

    private static final String[] TLS_PROTOCOLS = new String[] {
        TlsVersions.TLS1_3, TlsVersions.TLS1_2, TlsVersions.TLS1_1, TlsVersions.TLS1
    };

    private static final String[] OLD_TLS_PROTOCOLS = new String[] { TlsVersions.TLS1_1, TlsVersions.TLS1 };

    private static final ServerVersion VER_5_6_0 = ServerVersion.create(5, 6, 0);

    private static final ServerVersion VER_5_6_46 = ServerVersion.create(5, 6, 46);

    private static final ServerVersion VER_5_7_0 = ServerVersion.create(5, 7, 0);

    private static final ServerVersion VER_5_7_28 = ServerVersion.create(5, 7, 28);

    private final ConnectionContext context;

    private final MySqlSslConfiguration ssl;

    private SSLEngine sslEngine;

    SslBridgeHandler(ConnectionContext context, MySqlSslConfiguration ssl) {
        this.context = requireNonNull(context, "context must not be null");
        this.ssl = requireNonNull(ssl, "ssl must not be null");
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        if (ssl.getSslMode() == SslMode.TUNNEL) {
            handleSslState(ctx, SslState.BRIDGING);
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof SslState) {
            handleSslState(ctx, (SslState) evt);
            // Ignore event trigger for next handler, because it used only by this handler.
            return;
        } else if (evt instanceof SslHandshakeCompletionEvent) {
            handleSslCompleted(ctx, (SslHandshakeCompletionEvent) evt);
        }

        super.userEventTriggered(ctx, evt);
    }

    private void handleSslCompleted(ChannelHandlerContext ctx, SslHandshakeCompletionEvent evt) {
        if (!evt.isSuccess()) {
            ctx.fireExceptionCaught(evt.cause());
            return;
        }

        SslMode mode = ssl.getSslMode();

        if (mode.verifyIdentity()) {
            SSLEngine sslEngine = this.sslEngine;
            if (sslEngine == null) {
                ctx.fireExceptionCaught(new IllegalStateException(
                    "sslEngine must not be null when verify identity"));
                return;
            }

            String host = ((InetSocketAddress) ctx.channel().remoteAddress()).getHostName();

            if (!hostnameVerifier().verify(host, sslEngine.getSession())) {
                // Verify failed, emit an exception.
                ctx.fireExceptionCaught(new SSLException("The hostname '" + host +
                    "' could not be verified"));
                return;
            }
            // Otherwise, verify success, continue subsequence logic.
        }

        if (mode != SslMode.TUNNEL) {
            ctx.fireChannelRead(SyntheticSslResponseMessage.INSTANCE);
        }

        // Remove self because it is useless. (kick down the ladder!)
        logger.debug("SSL handshake completed, remove SSL bridge in pipeline");
        ctx.pipeline().remove(NAME);
    }

    private void handleSslState(ChannelHandlerContext ctx, SslState state) {
        switch (state) {
            case BRIDGING:
                logger.debug("SSL event triggered, enable SSL handler to pipeline");

                SslProvider sslProvider = SslProvider.builder()
                    .sslContext(MySqlSslContextSpec.forClient(ssl, context.getServerVersion()))
                    .build();
                SslHandler sslHandler = sslProvider.getSslContext().newHandler(ctx.alloc());

                this.sslEngine = sslHandler.engine();

                ctx.pipeline().addBefore(NAME, SSL_NAME, sslHandler);

                break;
            case UNSUPPORTED:
                // Remove self because it is useless. (kick down the ladder!)
                logger.debug("Server unsupported SSL, remove SSL bridge in pipeline");
                ctx.pipeline().remove(NAME);
                break;
        }
        // Ignore another unknown SSL states because it should not throw an exception.
    }

    private HostnameVerifier hostnameVerifier() {
        HostnameVerifier verifier = ssl.getSslHostnameVerifier();
        return verifier == null ? DefaultHostnameVerifier.INSTANCE : verifier;
    }

    private static boolean isCurrentTlsEnabled(ServerVersion version) {
        // See also https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-using-ssl.html
        // Quoting fragment: TLSv1,TLSv1.1,TLSv1.2,TLSv1.3 for MySQL Community Servers 8.0, 5.7.28 and
        // later, and 5.6.46 and later, and for all commercial versions of MySQL Servers.
        return version.isGreaterThanOrEqualTo(VER_5_7_28)
            || (version.isGreaterThanOrEqualTo(VER_5_6_46) && version.isLessThan(VER_5_7_0))
            || (version.isGreaterThanOrEqualTo(VER_5_6_0) && version.isEnterprise());
    }

    private static final class MySqlSslContextSpec implements SslProvider.ProtocolSslContextSpec {

        private final SslContextBuilder builder;

        private MySqlSslContextSpec(SslContextBuilder builder) { this.builder = builder; }

        @Override
        public MySqlSslContextSpec configure(Consumer<SslContextBuilder> customizer) {
            requireNonNull(customizer, "customizer must not be null");

            customizer.accept(builder);

            return this;
        }

        @Override
        public SslContext sslContext() throws SSLException {
            return builder.build();
        }

        static MySqlSslContextSpec forClient(MySqlSslConfiguration ssl, ServerVersion version) {
            // Same default configuration as TcpSslContextSpec.
            SslContextBuilder builder = SslContextBuilder.forClient()
                .sslProvider(OpenSsl.isAvailable() ? OPENSSL : JDK)
                .ciphers(null, IdentityCipherSuiteFilter.INSTANCE)
                .applicationProtocolConfig(null);
            String[] tlsProtocols = ssl.getTlsVersion();

            if (tlsProtocols.length > 0) {
                builder.protocols(tlsProtocols);
            } else if (isCurrentTlsEnabled(version)) {
                builder.protocols(TLS_PROTOCOLS);
            } else {
                // Not sure if we need to check the JDK version, suggest not.
                logger.warn("MySQL {} does not support TLS1.2, TLS1.1 is disabled in latest JDKs", version);
                builder.protocols(OLD_TLS_PROTOCOLS);
            }

            String sslKey = ssl.getSslKey();

            if (sslKey != null) {
                CharSequence keyPassword = ssl.getSslKeyPassword();
                String sslCert = ssl.getSslCert();

                if (sslCert == null) {
                    throw new IllegalStateException("SSL key present but client cert does not exist");
                }

                builder.keyManager(new File(sslCert), new File(sslKey),
                    keyPassword == null ? null : keyPassword.toString());
            }

            if (ssl.getSslMode().verifyCertificate()) {
                String sslCa = ssl.getSslCa();

                if (sslCa != null) {
                    builder.trustManager(new File(sslCa));
                }
                // Otherwise, use default algorithm with trust manager.
            } else {
                builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
            }

            return new MySqlSslContextSpec(ssl.customizeSslContext(builder));
        }
    }
}
