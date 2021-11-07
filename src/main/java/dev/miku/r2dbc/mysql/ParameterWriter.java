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

package dev.miku.r2dbc.mysql;

import reactor.util.annotation.Nullable;

import java.io.Writer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * A writer for {@link MySqlParameter}s of parametrized statements with text-based protocol.
 */
public abstract class ParameterWriter extends Writer {

    /**
     * Writes a {@code null} value to current parameter, nothing else can be written before or after this.
     *
     * @throws IllegalStateException if parameters filled, or something was written before that.
     */
    public abstract void writeNull();

    /**
     * Writes a value of {@code int} to current parameter. If current mode is string mode, it will write as a
     * string like {@code write(String.valueOf(value))}. If it write as a numeric, nothing else can be written
     * before or after this.
     *
     * @param value the value of {@code int}.
     * @throws IllegalStateException if parameters filled, or a non-string value was written before numeric.
     */
    public abstract void writeInt(int value);

    /**
     * Writes a value of {@code long} to current parameter. If current mode is string mode, it will write as a
     * string like {@code write(String.valueOf(value))}. If it write as a numeric, nothing else can be written
     * before or after this.
     *
     * @param value the value of {@code long}.
     * @throws IllegalStateException if parameters filled, or something was written before that numeric.
     */
    public abstract void writeLong(long value);

    /**
     * Writes a value of {@link BigInteger} to current parameter. If current mode is string mode, it will
     * write as a string like {@code write(value.toString())}. If it write as a numeric, nothing else can be
     * written before or after this.
     *
     * @param value the value of {@link BigInteger}.
     * @throws IllegalArgumentException the {@code value} is {@code null}.
     * @throws IllegalStateException    if parameters filled, or something was written before that numeric.
     */
    public abstract void writeBigInteger(BigInteger value);

    /**
     * Writes a value of {@code float} to current parameter. If current mode is string mode, it will write as
     * a string like {@code write(String.valueOf(value))}. If it write as a numeric, nothing else can be
     * written before or after this.
     *
     * @param value the value of {@code float}.
     * @throws IllegalStateException if parameters filled, or something was written before that numeric.
     */
    public abstract void writeFloat(float value);

    /**
     * Writes a value of {@code double} to current parameter. If current mode is string mode, it will write as
     * a string like {@code write(String.valueOf(value))}. If it write as a numeric, nothing else can be
     * written before or after this.
     *
     * @param value the value of {@code double}.
     * @throws IllegalStateException if parameters filled, or something was written before that numeric.
     */
    public abstract void writeDouble(double value);

    /**
     * Writes a value of {@link BigDecimal} to current parameter. If current mode is string mode, it will
     * write as a string like {@code write(value.toString())}. If it write as a numeric, nothing else can be
     * written before or after this.
     *
     * @param value the value of {@link BigDecimal}.
     * @throws IllegalArgumentException the {@code value} is {@code null}.
     * @throws IllegalStateException    if parameters filled, or something was written before that numeric.
     */
    public abstract void writeBigDecimal(BigDecimal value);

    /**
     * Writes a value of binary data to current parameter with binary encoding. Only binary encoding can be
     * written before or after this.
     *
     * @param bit the binary data.
     * @throws IllegalStateException if parameters filled, or other encoding was written before that.
     */
    public abstract void writeBinary(boolean bit);

    /**
     * Writes a value of binary data to current parameter with hex encoding. Only hex encoding can be written
     * before or after this.
     * <p>
     * Note: to write binary data with text protocol, Hex or Base64 encoding should be used. And MySQL 5.5
     * Community Edition does not support Base64.
     *
     * @param buffer the binary data.
     * @throws IllegalArgumentException if {@code buffer} is {@code null}.
     * @throws IllegalStateException    if parameters filled, or other encoding was written before that.
     */
    public abstract void writeHex(ByteBuffer buffer);

    /**
     * Writes a value of binary data to current parameter with hex encoding. Only hex encoding can be written
     * before or after this.
     * <p>
     * Note: write a binary data with text protocol need use Hex or Base64 encoding. And MySQL 5.5 Community
     * Edition does not support Base64.
     *
     * @param bytes the binary data.
     * @throws IllegalArgumentException if {@code bytes} is {@code null}.
     * @throws IllegalStateException    if parameters filled, or other encoding was written before that.
     */
    public abstract void writeHex(byte[] bytes);

    /**
     * Writes a value of binary data to current parameter with hex encoding. Only hex encoding can be written
     * before or after this.
     * <p>
     * Note: write a binary data with text protocol need use Hex or Base64 encoding. And MySQL 5.5 Community
     * Edition does not support Base64.
     *
     * @param bits the binary data bitmap, see also {@link Long#toHexString(long)}.
     * @throws IllegalArgumentException if {@code bytes} is {@code null}.
     * @throws IllegalStateException    if parameters filled, or other encoding was written before that.
     */
    public abstract void writeHex(long bits);

    /**
     * Mark the following parameter is a string, prepare and wrap the placeholder with {@code '}. If this
     * writer has been written a string value or marked as string mode, it will do nothing. Only string (or
     * stringify numeric) can be written before this.
     * <p>
     * It is useful for a string parameter starts with a number, like date or time. Also be used by reactive
     * streaming string parameter, like {@code Clob}.
     *
     * @throws IllegalStateException if parameters filled, or a non-string value was written before calling.
     */
    public abstract void startString();

    /**
     * Mark the following parameter is a hex byte buffer, replace the placeholder with {@code x'}. If this
     * writer has been written a hex value or marked as hex mode, it will do nothing. Only hex value can be
     * written before this.
     * <p>
     * It is useful for reactive streaming byte buffer parameter, like {@code Blob}.
     *
     * @throws IllegalStateException if parameters filled, or a non-hex value was written before calling.
     */
    public abstract void startHex();

    /**
     * Writes a character as a string. Only string (or stringify numeric) can be written before this. All
     * character in string mode will be automatically escaped.
     *
     * @param c an {@code int} specifying the character.
     * @throws IllegalStateException if parameters filled, or a non-string value was written before that.
     */
    @Override
    public abstract void write(int c);

    /**
     * Writes a character as a string. Only string (or stringify numeric) can be written before this. All
     * character in string mode will be automatically escaped.
     *
     * @param c the character.
     * @throws IllegalStateException if parameters filled, or a non-string value was written before that.
     */
    @Override
    public abstract ParameterWriter append(char c);

    /**
     * Writes a {@link CharSequence} as a string. Only string (or stringify numeric) can be written before
     * this. All character in string mode will be automatically escaped.
     *
     * @param csq the {@link CharSequence}.
     * @throws IllegalStateException if parameters filled, or a non-string value was written before that.
     */
    @Override
    public abstract ParameterWriter append(@Nullable CharSequence csq);

    /**
     * Writes a subsequence of a {@link CharSequence} as a string. Only string (or stringify numeric) can be
     * written before this. All character in string mode will be automatically escaped.
     *
     * @param csq   the {@link CharSequence}.
     * @param start index of the first position in the subsequence of the {@code csq}.
     * @param end   index of the sentinel position in the subsequence of the {@code csq}.
     * @throws IllegalStateException if parameters filled, or a non-string value was written before that.
     */
    @Override
    public abstract ParameterWriter append(@Nullable CharSequence csq, int start, int end);

    /**
     * Writes a {@link String}. Only string (or stringify numeric) can be written before this. All character
     * in string mode will be automatically escaped.
     *
     * @param str the {@link String}.
     * @throws IllegalStateException if parameters filled, or a non-string value was written before that.
     */
    @Override
    public abstract void write(@Nullable String str);

    /**
     * Writes a substring of a {@link String}. Only string (or stringify numeric) can be written before this.
     * All character in string mode will be automatically escaped.
     *
     * @param str the {@link String}.
     * @param off index of the first position in the substring of the {@code str}.
     * @param len length of the substring.
     * @throws IllegalStateException if parameters filled, or a non-string value was written before that.
     */
    @Override
    public abstract void write(@Nullable String str, int off, int len);

    /**
     * Writes a character array as a string. Only string (or stringify numeric) can be written before this.
     * All character in string mode will be automatically escaped.
     *
     * @param c the character array.
     * @throws IllegalStateException if parameters filled, or a non-string value was written before that.
     */
    @Override
    public abstract void write(@Nullable char[] c);

    /**
     * Writes a subsequence of a character array as a string. Only string (or stringify numeric) can be
     * written before this. All character in string mode will be automatically escaped.
     *
     * @param c   the character array.
     * @param off index of the first position in the subsequence of the {@code c}.
     * @param len length of the subsequence.
     * @throws IllegalStateException if parameters filled, or a non-string value was written before that.
     */
    @Override
    public abstract void write(@Nullable char[] c, int off, int len);

    /**
     * NOOP. This writer is a buffer writer.
     */
    @Override
    public final void flush() { }

    /**
     * NOOP. This writer is a buffer writer.
     */
    @Override
    public final void close() { }
}
