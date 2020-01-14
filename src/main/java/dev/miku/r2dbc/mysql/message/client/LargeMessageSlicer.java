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

package dev.miku.r2dbc.mysql.message.client;

import dev.miku.r2dbc.mysql.constant.Envelopes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.ReferenceCountUtil;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.FluxSink;

import java.util.ArrayList;
import java.util.List;

final class LargeMessageSlicer implements CoreSubscriber<ByteBuf> {

    private final ByteBufAllocator allocator;

    private final FluxSink<ByteBuf> sink;

    private int nowBytes = 0;

    private List<ByteBuf> now = null;

    LargeMessageSlicer(ByteBufAllocator allocator, FluxSink<ByteBuf> sink) {
        this.allocator = allocator;
        this.sink = sink;
    }

    @Override
    public void onSubscribe(Subscription s) {
        s.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(ByteBuf buf) {
        if (!buf.isReadable())  {
            // Ignore empty buffer.
            buf.release();
            return;
        }

        if (now == null) {
            onNullNext(buf);
        } else {
            int needBytes = Envelopes.MAX_ENVELOPE_SIZE - nowBytes;

            if (buf.readableBytes() < needBytes) {
                // Must less than sliceBytes
                nowBytes += buf.readableBytes();
                now.add(buf);
            } else {
                now.add(buf.readRetainedSlice(needBytes));
                sink.next(mergeNow());
                now = null;
                nowBytes = 0;

                onNullNext(buf);
            }
        }
    }

    @Override
    public void onError(Throwable cause) {
        try {
            sink.error(cause);
        } finally {
            releaseNow();
        }
    }

    @Override
    public void onComplete() {
        try {
            if (now == null) {
                // Complete envelope.
                sink.next(allocator.buffer(0, 0));
            } else {
                sink.next(mergeNow());
                now = null;
            }
            sink.complete();
        } finally {
            releaseNow();
        }
    }

    @SuppressWarnings("ForLoopReplaceableByForEach")
    private void releaseNow() {
        if (now != null) {
            int size = now.size();
            for (int i = 0; i < size; ++i) {
                ReferenceCountUtil.safeRelease(now.get(i));
            }
        }
    }

    private ByteBuf mergeNow() {
        int size = now.size();

        if (size == 1) {
            return now.get(0);
        }

        int i = 0;
        CompositeByteBuf result = allocator.compositeBuffer(size);

        try {
            for (; i < size; ++i) {
                result.addComponent(true, now.get(i));
            }

            return result;
        } catch (Throwable e) {
            ReferenceCountUtil.safeRelease(result);

            for (; i < size; ++i) {
                ReferenceCountUtil.safeRelease(now.get(i));
            }

            throw e;
        }
    }

    private void onNullNext(ByteBuf buf) {
        while (buf.readableBytes() >= Envelopes.MAX_ENVELOPE_SIZE) {
            sink.next(buf.readRetainedSlice(Envelopes.MAX_ENVELOPE_SIZE));
        }

        if (buf.isReadable()) {
            // Must less than sliceBytes
            now = new ArrayList<>();
            nowBytes = buf.readableBytes();
            now.add(buf);
        } else {
            buf.release();
        }
    }
}
