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

package dev.miku.r2dbc.mysql;

import reactor.util.annotation.Nullable;

import java.io.Writer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * A writer for {@link Parameter}s of parametrized statements with text-based protocol.
 */
public abstract class ParameterWriter extends Writer {

    public abstract void writeNull();

    public abstract void writeInt(int value);

    public abstract void writeLong(long value);

    /**
     * @param value the big integer.
     * @throws IllegalArgumentException the {@code value} is {@code null}.
     */
    public abstract void writeBigInteger(BigInteger value);

    public abstract void writeFloat(float value);

    public abstract void writeDouble(double value);

    /**
     * @param value the big decimal number.
     * @throws IllegalArgumentException the {@code value} is {@code null}.
     */
    public abstract void writeBigDecimal(BigDecimal value);

    public abstract void writeBinary(boolean bit);

    /**
     * Write a binary data with hex encoding.
     * <p>
     * Note: write a binary data with text protocol need use Hex or Base64 encoding.
     * And MySQL 5.5 Community Edition does not support Base64.
     *
     * @param buffer the binary data.
     * @throws IllegalArgumentException if {@code buffer} is {@code null}.
     */
    public abstract void writeHex(ByteBuffer buffer);

    public abstract void writeHex(byte[] bytes);

    /**
     * Mark the following parameter is a string, prepare and wrap the
     * placeholder with {@code '}.
     * <p>
     * It is useful for a string parameter starts with a number, like date or time.
     * Also be used by reactive streaming string parameter, like {@code Clob}.
     */
    public abstract void startString();

    /**
     * Mark the following parameter is a hex byte buffer, replace the placeholder
     * with {@code x'}.
     * <p>
     * It is useful for reactive streaming byte buffer parameter, like {@code Blob}.
     */
    public abstract void startHex();

    @Override
    public abstract void write(int c);

    @Override
    public abstract ParameterWriter append(char c);

    @Override
    public abstract ParameterWriter append(@Nullable CharSequence csq);

    @Override
    public abstract ParameterWriter append(@Nullable CharSequence csq, int start, int end);

    @Override
    public abstract void write(@Nullable String str);

    @Override
    public abstract void write(@Nullable String str, int off, int len);

    @Override
    public abstract void write(@Nullable char[] c);

    @Override
    public abstract void write(@Nullable char[] c, int off, int len);

    @Override
    public final void flush() {
    }

    @Override
    public final void close() {
    }
}
