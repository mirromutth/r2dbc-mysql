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

import io.github.mirromutth.r2dbc.mysql.core.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.message.client.ClientMessage;
import io.github.mirromutth.r2dbc.mysql.message.client.SimpleQueryMessage;
import io.github.mirromutth.r2dbc.mysql.message.header.SequenceIdProvider;
import io.github.mirromutth.r2dbc.mysql.message.server.DecodeContext;
import io.github.mirromutth.r2dbc.mysql.message.server.EofMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.ErrorMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.OkMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.ServerMessage;
import io.github.mirromutth.r2dbc.mysql.message.server.ServerMessageDecoder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * Client/server messages encode/decode logic.
 */
final class MessageDuplexCodec extends ChannelDuplexHandler {

    static final String NAME = "R2dbcMySqlMessageDuplexCodec";

    private static final Logger logger = LoggerFactory.getLogger(MessageDuplexCodec.class);

    private final SequenceIdProvider sequenceIdProvider = SequenceIdProvider.atomic();

    private final MySqlSession session;

    private final AtomicBoolean closing;

    private final ServerMessageDecoder decoder = new ServerMessageDecoder();

    private final AtomicReference<DecodeContext> decodeContext = new AtomicReference<>(DecodeContext.connection());

    MessageDuplexCodec(MySqlSession session, AtomicBoolean closing) {
        this.session = requireNonNull(session, "session must not be null");
        this.closing = requireNonNull(closing, "closing must not be null");
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            ServerMessage message = decoder.decode((ByteBuf) msg, sequenceIdProvider, session, this.decodeContext.get());

            if (message != null) {
                doOnRead(message);
                super.channelRead(ctx, message);
            }
        } else {
            if (logger.isWarnEnabled()) {
                logger.warn("Unknown message type {} on reading", msg.getClass());
            }
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (msg instanceof ClientMessage) {
            ClientMessage message = (ClientMessage) msg;
            doOnWrite(message);

            if (promise == null) {
                message.writeAndFlush(ctx, this.sequenceIdProvider, this.session);
            } else {
                message.writeAndFlush(ctx, this.sequenceIdProvider, this.session);
            }
        } else {
            if (logger.isWarnEnabled()) {
                logger.warn("Unknown message type {} on writing", msg.getClass());
            }
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        this.decoder.dispose();

        // Server has closed the connection without us wanting to close it
        // Typically happens if we send data asynchronously (i.e. previous command didn't complete).
        if (closing.compareAndSet(false, true)) {
            logger.warn("Connection has been closed by peer");
        }

        ctx.fireChannelInactive();
    }

    private void doOnWrite(ClientMessage msg) {
        if (msg instanceof SimpleQueryMessage) {
            DecodeContext context = DecodeContext.textResult();
            this.decodeContext.set(context);
            if (logger.isDebugEnabled()) {
                logger.debug("Decode context change to {}", context);
            }
        }
    }

    private void doOnRead(ServerMessage msg) {
        if (msg instanceof OkMessage) {
            OkMessage message = (OkMessage) msg;
            int warnings = message.getWarnings();

            this.session.setServerStatuses(message.getServerStatuses());

            if (warnings > 0 && logger.isWarnEnabled()) {
                logger.warn("MySQL server has {} warnings", warnings);
            }

            DecodeContext context = DecodeContext.command();
            this.decodeContext.set(context);
            if (logger.isDebugEnabled()) {
                logger.debug("Decode context change to {}", context);
            }
        } else if (msg instanceof ErrorMessage) {
            handleErrorMessage((ErrorMessage) msg);
        } else if (msg instanceof EofMessage) {
            EofMessage message = (EofMessage) msg;
            int warnings = message.getWarnings();

            this.session.setServerStatuses(message.getServerStatuses());

            if (warnings > 0 && logger.isWarnEnabled()) {
                logger.warn("MySQL server has {} warnings", warnings);
            }
        }
    }

    private void handleErrorMessage(ErrorMessage message) {
        boolean warning = logger.isWarnEnabled();

        if (warning) {
            logger.warn("Error: error code {}, sql state: {}, message: {}", message.getErrorCode(), message.getSqlState(), message.getErrorMessage());
        }

        DecodeContext currentContext = this.decodeContext.get();
        DecodeContext nextContext = currentContext.onError();

        // Use native equals for CAS, not Object.equals
        if (currentContext != nextContext) {
            if (this.decodeContext.compareAndSet(currentContext, nextContext)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Decode context change to {}", nextContext);
                }
            } else {
                if (warning) {
                    logger.warn("Change decode context failed when error message come");
                }
            }
        }
    }
}
