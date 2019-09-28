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

package dev.miku.r2dbc.mysql.codec.lob;

import dev.miku.r2dbc.mysql.util.ServerVersion;
import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;

/**
 * A utility for create {@link Clob} and {@link Blob} by single or multiple {@link ByteBuf}(s).
 */
public final class LobUtils {

    public static Blob createBlob(ByteBuf buf) {
        buf.retain();
        try {
            return new SingletonBlob(new Node(buf));
        } catch (Throwable e) {
            buf.release();
            throw e;
        }
    }

    public static Blob createBlob(ByteBuf[] buffers) {
        int size = buffers.length;
        Node[] nodes = new Node[size];

        try {
            retainAll(nodes, buffers, size);

            return new MultiBlob(nodes);
        } catch (Throwable e) {
            Node.releaseAll(nodes);
            throw e;
        }
    }

    public static Clob createClob(ByteBuf buf, int collationId, ServerVersion version) {
        buf.retain();
        try {
            return new SingletonClob(new Node(buf), collationId, version);
        } catch (Throwable e) {
            buf.release();
            throw e;
        }
    }

    public static Clob createClob(ByteBuf[] buffers, int collationId, ServerVersion version) {
        int size = buffers.length;
        Node[] nodes = new Node[size];

        try {
            retainAll(nodes, buffers, size);

            return new MultiClob(nodes, collationId, version);
        } catch (Throwable e) {
            Node.releaseAll(nodes);
            throw e;
        }
    }

    private static void retainAll(Node[] nodes, ByteBuf[] buffers, int size) {
        for (int i = 0; i < size; ++i) {
            nodes[i] = new Node(buffers[i].retain());
        }
    }

    private LobUtils() {

    }
}
