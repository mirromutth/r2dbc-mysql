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

import dev.miku.r2dbc.mysql.constant.Capabilities;
import dev.miku.r2dbc.mysql.message.client.PrepareQueryMessage;
import dev.miku.r2dbc.mysql.message.client.PreparedFetchMessage;
import dev.miku.r2dbc.mysql.ConnectionContext;
import dev.miku.r2dbc.mysql.message.client.ClientMessage;
import dev.miku.r2dbc.mysql.message.client.SslRequest;
import dev.miku.r2dbc.mysql.message.header.SequenceIdProvider;
import dev.miku.r2dbc.mysql.message.server.ColumnCountMessage;
import dev.miku.r2dbc.mysql.message.server.CompleteMessage;
import dev.miku.r2dbc.mysql.message.server.DecodeContext;
import dev.miku.r2dbc.mysql.message.server.ErrorMessage;
import dev.miku.r2dbc.mysql.message.server.PreparedOkMessage;
import dev.miku.r2dbc.mysql.message.server.ServerMessage;
import dev.miku.r2dbc.mysql.message.server.ServerMessageDecoder;
import dev.miku.r2dbc.mysql.message.server.ServerStatusMessage;
import dev.miku.r2dbc.mysql.message.server.SyntheticMetadataMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.annotation.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * Client/server messages encode/decode logic.
 */
final class MessageDuplexCodec extends ChannelDuplexHandler {

    static final String NAME = "R2dbcMySqlMessageDuplexCodec";

    private static final Logger logger = LoggerFactory.getLogger(MessageDuplexCodec.class);

    private DecodeContext decodeContext = DecodeContext.connection();

    @Nullable
    private SequenceIdProvider.Linkable linkableIdProvider;

    private final ConnectionContext context;

    private final AtomicBoolean closing;

    private final RequestQueue requestQueue;

    private final ServerMessageDecoder decoder = new ServerMessageDecoder();

    MessageDuplexCodec(ConnectionContext context, AtomicBoolean closing, RequestQueue requestQueue) {
        this.context = requireNonNull(context, "context must not be null");
        this.closing = requireNonNull(closing, "closing must not be null");
        this.requestQueue = requireNonNull(requestQueue, "requestQueue must not be null");
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof Lifecycle) {
            if (Lifecycle.COMMAND == evt) {
                // Message sequence id always from 0 in command phase.
                this.linkableIdProvider = null;
            }
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof ByteBuf) {
            DecodeContext context = this.decodeContext;
            ServerMessage message = decoder.decode((ByteBuf) msg, this.context, context, this.linkableIdProvider);

            if (message != null) {
                handleDecoded(ctx, message);
            }
        } else if (msg instanceof ServerMessage) {
            ctx.fireChannelRead(msg);
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
            ((ClientMessage) msg).encode(ctx.alloc(), this.context)
                .subscribe(WriteSubscriber.create(ctx, promise, this.linkableIdProvider));

            if (msg instanceof PrepareQueryMessage) {
                setDecodeContext(DecodeContext.prepareQuery());
            } else if (msg instanceof PreparedFetchMessage) {
                setDecodeContext(DecodeContext.fetch());
            } else if (msg instanceof SslRequest) {
                ctx.channel().pipeline().fireUserEventTriggered(SslState.BRIDGING);
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
        decoder.dispose();
        requestQueue.dispose();

        // Server has closed the connection without us wanting to close it
        // Typically happens if we send data asynchronously (i.e. previous command didn't complete).
        if (closing.compareAndSet(false, true)) {
            logger.warn("Connection has been closed by peer");
        }

        ctx.fireChannelInactive();
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.linkableIdProvider = SequenceIdProvider.atomic();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        this.linkableIdProvider = null;
    }

    private void handleDecoded(ChannelHandlerContext ctx, ServerMessage msg) {
        if (msg instanceof ServerStatusMessage) {
            this.context.setServerStatuses(((ServerStatusMessage) msg).getServerStatuses());
        }

        if (msg instanceof CompleteMessage) {
            // Metadata EOF message will be not receive in here.
            setDecodeContext(DecodeContext.command());
        } else if (msg instanceof SyntheticMetadataMessage) {
            if (((SyntheticMetadataMessage) msg).isCompleted()) {
                setDecodeContext(DecodeContext.command());
            }
        } else if (msg instanceof ColumnCountMessage) {
            boolean deprecateEof = (this.context.getCapabilities() & Capabilities.DEPRECATE_EOF) != 0;
            setDecodeContext(DecodeContext.result(deprecateEof, ((ColumnCountMessage) msg).getTotalColumns()));
            return; // Done, no need use generic handle.
        } else if (msg instanceof PreparedOkMessage) {
            PreparedOkMessage message = (PreparedOkMessage) msg;
            int columns = message.getTotalColumns();
            int parameters = message.getTotalParameters();

            // For supports use server-preparing query for simple statements. The count of columns and
            // parameters may all be 0. All is 0 means no EOF message following.
            // columns + parameters > 0
            if (columns > -parameters) {
                boolean deprecateEof = (this.context.getCapabilities() & Capabilities.DEPRECATE_EOF) != 0;
                setDecodeContext(DecodeContext.preparedMetadata(deprecateEof, columns, parameters));
            } else {
                setDecodeContext(DecodeContext.command());

                // Prepared OK first, this is also generic handle.
                ctx.fireChannelRead(msg);
                // Metadata second, append a synthetic message following Prepare OK.
                ctx.fireChannelRead(SyntheticMetadataMessage.completedEmpty());

                return; // Done, no need use generic handle.
            }
        } else if (msg instanceof ErrorMessage) {
            setDecodeContext(DecodeContext.command());
        }

        // Generic handle.
        ctx.fireChannelRead(msg);
    }

    private void setDecodeContext(DecodeContext context) {
        this.decodeContext = context;
        if (logger.isDebugEnabled()) {
            logger.debug("Decode context change to {}", context);
        }
    }
}
