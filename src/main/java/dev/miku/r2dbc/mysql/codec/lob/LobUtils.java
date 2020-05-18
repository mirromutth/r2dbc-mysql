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

package dev.miku.r2dbc.mysql.codec.lob;

import dev.miku.r2dbc.mysql.util.ServerVersion;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;

import java.util.List;

/**
 * A utility for create {@link Clob} and {@link Blob} by single or multiple {@link ByteBuf}(s).
 */
public final class LobUtils {

    public static Blob createBlob(ByteBuf value) {
        ByteBuf buf = value.retain();

        try {
            return new SingletonBlob(buf);
        } catch (Throwable e) {
            buf.release();
            throw e;
        }
    }

    public static Blob createBlob(List<ByteBuf> value) {
        int size = value.size(), i = 0;

        try {
            for (; i < size; ++i) {
                value.get(i).retain();
            }

            return new MultiBlob(value);
        } catch (Throwable e) {
            for (int j = 0; j < i; ++j) {
                ReferenceCountUtil.safeRelease(value.get(j));
            }

            throw e;
        }
    }

    public static Clob createClob(ByteBuf value, int collationId, ServerVersion version) {
        ByteBuf buf = value.retain();

        try {
            return new SingletonClob(buf, collationId, version);
        } catch (Throwable e) {
            buf.release();
            throw e;
        }
    }

    public static Clob createClob(List<ByteBuf> buffers, int collationId, ServerVersion version) {
        int size = buffers.size(), i = 0;

        try {
            for (; i < size; ++i) {
                buffers.get(i).retain();
            }

            return new MultiClob(buffers, collationId, version);
        } catch (Throwable e) {
            for (int j = 0; j < i; ++j) {
                ReferenceCountUtil.safeRelease(buffers.get(j));
            }

            throw e;
        }
    }

    private LobUtils() {
    }
}
