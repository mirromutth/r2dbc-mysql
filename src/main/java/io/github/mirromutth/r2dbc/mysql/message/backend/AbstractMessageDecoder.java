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

package io.github.mirromutth.r2dbc.mysql.message.backend;

import io.github.mirromutth.r2dbc.mysql.constant.Envelopes;
import io.github.mirromutth.r2dbc.mysql.core.MySqlSession;
import io.netty.buffer.ByteBuf;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * Generic message decoder logic.
 */
abstract class AbstractMessageDecoder implements MessageDecoder {

    private final AtomicBoolean disposed = new AtomicBoolean(false);

    private final ByteBufJoiner joiner = ByteBufJoiner.wrapped();

    private final List<ByteBuf> parts = new ArrayList<>();

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public BackendMessage decode(ByteBuf envelope, AtomicInteger sequenceId, DecodeContext context, MySqlSession session) {
        if (!disposed.get()) {
            throw new IllegalStateException("decoder is already shutdown");
        }

        requireNonNull(envelope, "envelope must not be null");
        requireNonNull(sequenceId, "sequenceId must not be null");
        requireNonNull(context, "context must not be null");
        requireNonNull(session, "session must not be null");

        ByteBuf joined = processEnvelope(envelope, sequenceId);

        if (joined == null) {
            return null;
        }

        try {
            return decodeMessage(joined, context, session);
        } finally {
            joined.release();
        }
    }

    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override
    public void dispose() {
        if (disposed.compareAndSet(false, true)) {
            try {
                // Use the old-style for loop, see: https://github.com/netty/netty/issues/2642
                int size = parts.size();
                for (int i = 0; i < size; ++i) {
                    parts.get(i).release();
                }
            } finally {
                parts.clear();
            }
        }
    }

    @Override
    public boolean isDisposed() {
        return disposed.get();
    }

    @Nullable
    abstract BackendMessage decodeMessage(ByteBuf joined, DecodeContext context, MySqlSession session);

    @Nullable
    private ByteBuf processEnvelope(ByteBuf envelope, AtomicInteger sequenceId) {
        try {
            if (isLastPartWithRead(envelope)) {
                sequenceId.set(envelope.readUnsignedByte() + 1);
                ByteBuf joined = joiner.join(parts, envelope);

                envelope = null; // success, no need release

                return joined;
            } else {
                envelope.skipBytes(1); // sequence Id
                parts.add(envelope);
                envelope = null; // success, no need release
                return null;
            }
        } finally {
            if (envelope != null) {
                envelope.release();
            }
        }
    }

    private boolean isLastPartWithRead(ByteBuf partBuf) {
        int size = partBuf.readUnsignedMediumLE();
        return size < Envelopes.MAX_PART_SIZE;
    }
}
