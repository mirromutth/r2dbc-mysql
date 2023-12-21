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
import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.Parameter;

import java.util.BitSet;

/**
 * Codec for {@link BitSet}.
 */
final class ParameterCodec implements Codec< Parameter>, UsesCodecs {

    private Codecs codecs;

    @Override
    public Parameter decode(ByteBuf value, MySqlColumnMetadata metadata, Class<?> target, boolean binary, CodecContext context) {
        throw new UnsupportedOperationException("cannot decode " + Parameter.class);
    }

    @Override
    public boolean canDecode(MySqlColumnMetadata metadata, Class<?> target) {
        return false;
    }

    @Override
    public boolean canEncode(Object value) {
        return codecs != null && value instanceof Parameter;
    }

    @Override
    public dev.miku.r2dbc.mysql.Parameter encode(Object value, CodecContext context) {
            Parameter p = (Parameter) value;
            if (p.getValue() == null) {
                return NullParameter.INSTANCE;
            }
            return codecs.encode(p.getValue(), context);
    }

    public void setCodecs(Codecs codecs) {
        this.codecs = codecs;
    }
}
