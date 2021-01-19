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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;

/**
 * An implementation of {@link CoreSubscriber} for {@link ChannelHandlerContext} write and flush subscribed by
 * streaming {@link ByteBuf}s.
 * <p>
 * It ensures {@link #promise} will be complete.
 */
final class WriteSubscriber implements CoreSubscriber<ByteBuf> {

    private final ChannelHandlerContext ctx;

    private final ChannelPromise promise;

    WriteSubscriber(ChannelHandlerContext ctx, ChannelPromise promise) {
        this.ctx = ctx;
        this.promise = promise;
    }

    @Override
    public void onSubscribe(Subscription s) {
        s.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(ByteBuf buf) {
        ctx.write(buf);
    }

    @Override
    public void onError(Throwable cause) {
        // Ignore this cause for this promise because it is channel exception.
        promise.setSuccess();
        ctx.flush();
        ctx.fireExceptionCaught(cause);
    }

    @Override
    public void onComplete() {
        promise.setSuccess();
        ctx.flush();
    }
}
