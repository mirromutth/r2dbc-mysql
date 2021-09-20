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

import dev.miku.r2dbc.mysql.ConnectionContextTest;
import dev.miku.r2dbc.mysql.MySqlColumnMetadata;
import dev.miku.r2dbc.mysql.MySqlTypeMetadata;
import dev.miku.r2dbc.mysql.collation.CharCollation;
import dev.miku.r2dbc.mysql.constant.MySqlType;
import dev.miku.r2dbc.mysql.message.FieldValue;
import io.netty.buffer.PooledByteBufAllocator;
import io.r2dbc.spi.Nullability;
import org.junit.jupiter.api.Test;
import reactor.util.annotation.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link Codecs}.
 */
class CodecsTest {

    private static final Codecs CODECS = Codecs.builder(PooledByteBufAllocator.DEFAULT).build();

    private static final CodecContext CONTEXT = ConnectionContextTest.mock();

    private static final FieldValue NULL = FieldValue.nullField();

    @Test
    void decode() {
        assertThat(decodeNullField(MySqlType.TINYINT, byte.class)).isNull();
        assertThat(decodeNullField(MySqlType.SMALLINT, short.class)).isNull();
        assertThat(decodeNullField(MySqlType.INT, int.class)).isNull();
        assertThat(decodeNullField(MySqlType.BIGINT, long.class)).isNull();
        assertThat(decodeNullField(MySqlType.FLOAT, float.class)).isNull();
        assertThat(decodeNullField(MySqlType.DOUBLE, double.class)).isNull();
        assertThat(decodeNullField(MySqlType.BIT, boolean.class)).isNull();
    }

    @Nullable
    private static <T> T decodeNullField(MySqlType type, Class<T> clazz) {
        return CODECS.decode(NULL, new MockMySqlColumnMetadata(type), clazz, false, CONTEXT);
    }

    private static final class MockMySqlColumnMetadata implements MySqlColumnMetadata {

        private final MySqlType type;

        private MockMySqlColumnMetadata(MySqlType type) {
            this.type = type;
        }

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
        public CharCollation getCharCollation(CodecContext context) {
            return CharCollation.fromId(CharCollation.BINARY_ID, context.getServerVersion());
        }

        @Override
        public long getNativePrecision() {
            return 0;
        }

        @Override
        public Nullability getNullability() {
            return Nullability.NULLABLE;
        }
    }
}
