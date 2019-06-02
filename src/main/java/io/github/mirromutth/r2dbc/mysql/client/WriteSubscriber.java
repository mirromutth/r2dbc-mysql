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

import io.github.mirromutth.r2dbc.mysql.constant.Envelopes;
import io.github.mirromutth.r2dbc.mysql.message.header.SequenceIdProvider;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;

/**
 * An implementations of {@link CoreSubscriber} for {@link ChannelHandlerContext} write
 * and flush subscribed by streaming {@link ByteBuf}s.
 * <p>
 * It ensures {@link #promise} will be complete.
 */
final class WriteSubscriber implements CoreSubscriber<ByteBuf> {

    private final ChannelHandlerContext ctx;

    private final SequenceIdProvider provider;

    private final boolean reset;

    private final ChannelPromise promise;

    WriteSubscriber(ChannelHandlerContext ctx, SequenceIdProvider provider, boolean reset, ChannelPromise promise) {
        this.ctx = ctx;
        this.provider = provider;
        this.reset = reset;
        this.promise = promise;
    }

    @Override
    public void onSubscribe(Subscription s) {
        if (reset) {
            provider.reset();
        }
        s.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(ByteBuf buf) {
        ctx.write(ctx.alloc().buffer(Envelopes.PART_HEADER_SIZE, Envelopes.PART_HEADER_SIZE)
            .writeMediumLE(buf.readableBytes())
            .writeByte(provider.next()));
        ctx.writeAndFlush(buf);
    }

    @Override
    public void onError(Throwable cause) {
        ctx.fireExceptionCaught(cause);
        // Ignore this cause for this promise because it is channel exception.
        promise.setSuccess();
    }

    @Override
    public void onComplete() {
        promise.setSuccess();
    }
}
