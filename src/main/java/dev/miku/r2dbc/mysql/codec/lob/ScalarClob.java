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

import dev.miku.r2dbc.mysql.ServerVersion;
import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.Clob;

import java.util.List;

/**
 * Scalar {@link Clob} backed by an already received and de-chunked singleton
 * {@link ByteBuf} or {@link List} of {@link ByteBuf}.
 */
public abstract class ScalarClob implements Clob {

    public static ScalarClob retain(ByteBuf buf, int collationId, ServerVersion version) {
        buf.retain();
        try {
            return new SingletonClob(new Node(buf), collationId, version);
        } catch (Throwable e) {
            buf.release();
            throw e;
        }
    }

    public static ScalarClob retain(ByteBuf[] buffers, int collationId, ServerVersion version) {
        int size = buffers.length;
        int i = 0;
        Node[] nodes = new Node[size];

        try {
            for (; i < size; ++i) {
                nodes[i] = new Node(buffers[i].retain());
            }

            return new MultiClob(nodes, collationId, version);
        } catch (Throwable e) {
            for (int j = 0; j < i; ++j) {
                if (nodes[j] != null) {
                    nodes[j].safeDispose();
                }
            }
            throw e;
        }
    }
}
