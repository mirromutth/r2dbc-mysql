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

package dev.miku.r2dbc.mysql.codec;

import dev.miku.r2dbc.mysql.collation.CharCollation;
import dev.miku.r2dbc.mysql.constant.DataTypes;
import dev.miku.r2dbc.mysql.message.NormalFieldValue;
import dev.miku.r2dbc.mysql.message.ParameterValue;
import dev.miku.r2dbc.mysql.message.client.ParameterWriter;
import dev.miku.r2dbc.mysql.util.CodecUtils;
import dev.miku.r2dbc.mysql.util.ConnectionContext;
import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Mono;

/**
 * Codec for {@link String}.
 */
final class StringCodec extends AbstractClassedCodec<String> {

    static final StringCodec INSTANCE = new StringCodec();

    private StringCodec() {
        super(String.class);
    }

    @Override
    public String decode(NormalFieldValue value, FieldInformation info, Class<? super String> target, boolean binary, ConnectionContext context) {
        ByteBuf buf = value.getBufferSlice();

        if (!buf.isReadable()) {
            return "";
        }

        return buf.toString(CharCollation.fromId(info.getCollationId(), context.getServerVersion()).getCharset());
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof CharSequence;
    }

    @Override
    public ParameterValue encode(Object value, ConnectionContext context) {
        return new StringValue((CharSequence) value, context);
    }

    @Override
    protected boolean doCanDecode(FieldInformation info) {
        short type = info.getType();
        // Note: TEXT is also BLOB with char collation in MySQL.
        return (TypePredicates.isString(type) || TypePredicates.isLob(type)) && info.getCollationId() != CharCollation.BINARY_ID;
    }

    private static class StringValue extends AbstractParameterValue {

        private final CharSequence value;

        private final ConnectionContext context;

        private StringValue(CharSequence value, ConnectionContext context) {
            this.value = value;
            this.context = context;
        }

        @Override
        public Mono<Void> writeTo(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeCharSequence(value, context.getCollation()));
        }

        @Override
        public Mono<Void> writeTo(StringBuilder builder) {
            return Mono.fromRunnable(() -> {
                builder.append('\'');
                CodecUtils.appendEscape(builder, value);
                builder.append('\'');
            });
        }

        @Override
        public short getType() {
            return DataTypes.VARCHAR;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof StringValue)) {
                return false;
            }

            StringValue that = (StringValue) o;

            return value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }
}
