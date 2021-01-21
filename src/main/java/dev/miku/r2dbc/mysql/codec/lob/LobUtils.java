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

package dev.miku.r2dbc.mysql.codec.lob;

import dev.miku.r2dbc.mysql.ServerVersion;
import dev.miku.r2dbc.mysql.util.NettyBufferUtils;
import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;

import java.util.List;

/**
 * A utility for create {@link Clob} and {@link Blob} by single or multiple {@link ByteBuf}(s).
 */
public final class LobUtils {

    /**
     * Create a {@link Blob} from only one {@link ByteBuf}.
     *
     * @param value the only one {@link ByteBuf}.
     * @return the {@link Blob} from singleton.
     */
    public static Blob createBlob(ByteBuf value) {
        ByteBuf buf = value.retain();

        try {
            return new SingletonBlob(buf);
        } catch (Throwable e) {
            buf.release();
            throw e;
        }
    }

    /**
     * Create a {@link Blob} from multiple {@link ByteBuf}s.
     *
     * @param value the {@link ByteBuf}s list.
     * @return the {@link Blob} from multiple.
     */
    public static Blob createBlob(List<ByteBuf> value) {
        int size = value.size(), i = 0;

        try {
            for (; i < size; ++i) {
                value.get(i).retain();
            }

            return new MultiBlob(value);
        } catch (Throwable e) {
            NettyBufferUtils.releaseAll(value, i);
            throw e;
        }
    }

    /**
     * Create a {@link Clob} from only one {@link ByteBuf}.
     *
     * @param value       the only one {@link ByteBuf}.
     * @param collationId the identifier of {@code CharCollation}.
     * @param version     the version of MySQL server.
     * @return the {@link Clob} from singleton.
     */
    public static Clob createClob(ByteBuf value, int collationId, ServerVersion version) {
        ByteBuf buf = value.retain();

        try {
            return new SingletonClob(buf, collationId, version);
        } catch (Throwable e) {
            buf.release();
            throw e;
        }
    }

    /**
     * Create a {@link Clob} from multiple {@link ByteBuf}s.
     *
     * @param value       the {@link ByteBuf}s list.
     * @param collationId the identifier of {@code CharCollation}.
     * @param version     the version of MySQL server.
     * @return the {@link Clob} from multiple.
     */
    public static Clob createClob(List<ByteBuf> value, int collationId, ServerVersion version) {
        int size = value.size(), i = 0;

        try {
            for (; i < size; ++i) {
                value.get(i).retain();
            }

            return new MultiClob(value, collationId, version);
        } catch (Throwable e) {
            NettyBufferUtils.releaseAll(value, i);
            throw e;
        }
    }

    private LobUtils() { }
}
