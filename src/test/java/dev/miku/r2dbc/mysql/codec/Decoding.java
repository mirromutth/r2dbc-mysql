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

package dev.miku.r2dbc.mysql.codec;

import dev.miku.r2dbc.mysql.MySqlColumnMetadata;
import dev.miku.r2dbc.mysql.MySqlTypeMetadata;
import dev.miku.r2dbc.mysql.collation.CharCollation;
import dev.miku.r2dbc.mysql.constant.MySqlType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.DefaultByteBufHolder;
import io.r2dbc.spi.Nullability;

/**
 * An element considers a decoding test case.
 *
 * @see CodecTestSupport#decoding
 */
final class Decoding extends DefaultByteBufHolder {

    private final Object value;

    private final MySqlType type;

    Decoding(ByteBuf buf, Object value, MySqlType type) {
        super(buf);

        this.value = value;
        this.type = type;
    }

    Object value() {
        return value;
    }

    MySqlColumnMetadata metadata() {
        return new MockMySqlColumnMetadata(type);
    }

    @Override
    public String toString() {
        return "Decoding{value=" + value + ", type=" + type + '}';
    }

    private static final class MockMySqlColumnMetadata implements MySqlColumnMetadata {

        private final MySqlType type;

        private MockMySqlColumnMetadata(MySqlType type) { this.type = type; }

        @Override
        public MySqlType getType() {
            return type;
        }

        @Override
        public String getName() {
            return "mock";
        }

        @Override
        public MySqlTypeMetadata getNativeTypeMetadata() {
            return null;
        }

        @Override
        public Nullability getNullability() {
            return Nullability.NON_NULL;
        }

        @Override
        public CharCollation getCharCollation(CodecContext context) {
            return context.getClientCollation();
        }

        @Override
        public long getNativePrecision() {
            return 0;
        }
    }
}
