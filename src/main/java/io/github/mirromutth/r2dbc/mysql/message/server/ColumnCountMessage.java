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

package io.github.mirromutth.r2dbc.mysql.message.server;

import io.github.mirromutth.r2dbc.mysql.internal.CodecUtils;
import io.netty.buffer.ByteBuf;

import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.require;

/**
 * A message that is start envelope for {@literal SELECT} query result,
 * {@link #totalColumns}  how many columns will be returned for the result.
 */
public final class ColumnCountMessage implements ServerMessage {

    private final int totalColumns;

    private ColumnCountMessage(int totalColumns) {
        require(totalColumns > 0, "totalColumns must be a positive integer");

        this.totalColumns = totalColumns;
    }

    public int getTotalColumns() {
        return totalColumns;
    }

    static ColumnCountMessage decode(ByteBuf buf) {
        return new ColumnCountMessage(Math.toIntExact(CodecUtils.readVarInt(buf)));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ColumnCountMessage)) {
            return false;
        }

        ColumnCountMessage that = (ColumnCountMessage) o;

        return totalColumns == that.totalColumns;

    }

    @Override
    public int hashCode() {
        return totalColumns;
    }

    @Override
    public String toString() {
        return "ColumnCountMessage{" +
            "totalColumns=" + totalColumns +
            '}';
    }
}
