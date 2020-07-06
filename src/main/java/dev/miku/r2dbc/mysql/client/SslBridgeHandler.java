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

package dev.miku.r2dbc.mysql.client;

import dev.miku.r2dbc.mysql.MySqlSslConfiguration;
import dev.miku.r2dbc.mysql.ServerVersion;
import dev.miku.r2dbc.mysql.constant.SslMode;
import dev.miku.r2dbc.mysql.constant.TlsVersions;
import dev.miku.r2dbc.mysql.ConnectionContext;
import dev.miku.r2dbc.mysql.message.server.SyntheticSslResponseMessage;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.netty.tcp.SslProvider;

import javax.net.ssl.SSLEngine;
import java.io.File;
import java.net.InetSocketAddress;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

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

    private final ConnectionContext context;

    private final boolean verifyIdentity;

    private final MySqlSslConfiguration ssl;

    private SSLEngine sslEngine;

    SslBridgeHandler(ConnectionContext context, MySqlSslConfiguration ssl) {
        this.context = requireNonNull(context, "context must not be null");
        this.ssl = requireNonNull(ssl, "ssl must not be null");
        this.verifyIdentity = ssl.getSslMode().verifyIdentity();
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

        if (verifyIdentity) {
            SSLEngine sslEngine = this.sslEngine;
            String hostname = ((InetSocketAddress) ctx.channel().remoteAddress()).getHostName();

            if (sslEngine == null) {
                ctx.fireExceptionCaught(new IllegalStateException("sslEngine must not be null when verify identity"));
                return;
            }

            try {
                MySqlHostVerifier.accept(hostname, sslEngine.getSession());
            } catch (Exception e) {
                ctx.fireExceptionCaught(e);
                return;
            }
        }

        if (ssl.getSslMode() != SslMode.TUNNEL) {
            ctx.fireChannelRead(SyntheticSslResponseMessage.getInstance());
        }

        // Remove self because it is useless. (kick down the ladder!)
        logger.debug("SSL handshake completed, remove SSL bridge in pipeline");
        ctx.pipeline().remove(NAME);
    }

    private void handleSslState(ChannelHandlerContext ctx, SslState state) {
        switch (state) {
            case BRIDGING:
                logger.debug("SSL event triggered, enable SSL handler to pipeline");

                MySqlSslConfiguration ssl = this.ssl;

                if (ssl == null) {
                    ctx.fireExceptionCaught(new IllegalStateException("The SSL bridge has used, cannot build SSL handler twice"));
                    return;
                }

                SslProvider sslProvider = buildProvider(ssl, context.getServerVersion());
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
        // Ignore another custom SSL states because they are useless.
    }

    private static SslProvider buildProvider(MySqlSslConfiguration ssl, ServerVersion version) {
        return SslProvider.builder()
            .sslContext(buildContext(ssl, version))
            .defaultConfiguration(SslProvider.DefaultConfigurationType.TCP)
            .build();
    }

    private static SslContextBuilder buildContext(MySqlSslConfiguration ssl, ServerVersion version) {
        SslContextBuilder builder = withTlsVersion(SslContextBuilder.forClient(), ssl, version);
        String sslKey = ssl.getSslKey();

        if (sslKey != null) {
            CharSequence keyPassword = ssl.getSslKeyPassword();
            String sslCert = ssl.getSslCert();

            if (sslCert == null) {
                throw new IllegalStateException("SSL key param requires but SSL cert param to be present");
            }

            builder.keyManager(new File(sslCert), new File(sslKey), keyPassword == null ? null : keyPassword.toString());
        }

        SslMode mode = ssl.getSslMode();
        if (mode.verifyCertificate()) {
            String sslCa = ssl.getSslCa();

            if (sslCa != null) {
                builder.trustManager(new File(sslCa));
            }
            // Otherwise use default algorithm with trust manager.
        } else {
            builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
        }

        return ssl.customizeSslContext(builder);
    }

    private static SslContextBuilder withTlsVersion(SslContextBuilder builder, MySqlSslConfiguration ssl, ServerVersion version) {
        String[] tlsProtocols = ssl.getTlsVersion();

        if (tlsProtocols.length > 0) {
            builder.protocols(tlsProtocols);
        } else if (isEnabledTls1_2(version)) {
            builder.protocols(TlsVersions.TLS1, TlsVersions.TLS1_1, TlsVersions.TLS1_2);
        } else {
            builder.protocols(TlsVersions.TLS1, TlsVersions.TLS1_1);
        }

        return builder;
    }

    private static boolean isEnabledTls1_2(ServerVersion version) {
        return version.isGreaterThanOrEqualTo(TLS1_2_COMMUNITY_VER) || (version.isGreaterThanOrEqualTo(TLS1_2_ENTERPRISE_VER) && version.isEnterprise());
    }
}
