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

package dev.miku.r2dbc.mysql.authentication;

import dev.miku.r2dbc.mysql.collation.CharCollation;
import reactor.util.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;
import static dev.miku.r2dbc.mysql.util.InternalArrays.EMPTY_BYTES;

/**
 * An implementation of {@link MySqlAuthProvider} for type "mysql_old_password".
 */
final class OldAuthProvider implements MySqlAuthProvider {

    static final OldAuthProvider INSTANCE = new OldAuthProvider();

    private static final int MAX_SALT_LENGTH = 8;

    private static final long FIRST_HASHING = 0x50305735L;

    private static final long SECOND_HASHING = 0x12345671L;

    private static final int SUM_INIT_VALUE = 7;

    private static final int HASH_MARK = 0x3F;

    private static final long MOD = 0x3FFFFFFFL;

    private static final int SEED_INC = 0x21;

    private static final int SEED_MULTIPLIER = 3;

    private static final int RESULT_INC = 0x40;

    private static final int RESULT_MULTIPLIER = 0x1F;

    @Override
    public boolean isSslNecessary() {
        return false;
    }

    @Override
    public byte[] authentication(@Nullable CharSequence password, byte[] salt, CharCollation collation) {
        if (password == null || password.length() <= 0) {
            return EMPTY_BYTES;
        }

        requireNonNull(collation, "collation must not be null when password exists");

        Charset charset = collation.getCharset();
        String saltString;
        byte[] challenge;

        if (salt.length == 0) {
            saltString = "";
            challenge = EMPTY_BYTES;
        } else {
            String newString = new String(salt, charset);

            if (newString.length() > MAX_SALT_LENGTH) {
                saltString = newString.substring(0, MAX_SALT_LENGTH);
                challenge = saltString.getBytes(charset);
            } else {
                saltString = newString;
                challenge = salt;
            }
        }

        // Authentication results
        long authentications = hashing(challenge);

        // Messages results
        long messages = hashing(encodeNoWhitespace(password, charset));

        long firstSeed = (firstPart(authentications) ^ firstPart(messages)) % MOD;
        long secondSeed = (secondPart(authentications) ^ secondPart(messages)) % MOD;
        int stringSize = saltString.length();
        char[] results = new char[stringSize];

        for (int i = 0; i < stringSize; ++i) {
            firstSeed = ((firstSeed * SEED_MULTIPLIER) + secondSeed) % MOD;
            secondSeed = (firstSeed + secondSeed + SEED_INC) % MOD;
            results[i] = (char) (byte)
                Math.floor(((((double) firstSeed) / MOD) * RESULT_MULTIPLIER) + RESULT_INC);
        }

        long lastSeed = ((firstSeed * SEED_MULTIPLIER) + secondSeed) % MOD;
        char mark = (char) (byte) Math.floor((((double) lastSeed) / MOD) * RESULT_MULTIPLIER);

        for (int i = 0; i < stringSize; i++) {
            results[i] ^= mark;
        }

        return AuthUtils.encodeTerminal(CharBuffer.wrap(results), charset);
    }

    @Override
    public MySqlAuthProvider next() {
        return this;
    }

    @Override
    public String getType() {
        return MYSQL_OLD_PASSWORD;
    }

    private static byte[] encodeNoWhitespace(CharSequence password, Charset charset) {
        int size = password.length();
        StringBuilder builder = new StringBuilder(size);

        for (int i = 0; i < size; ++i) {
            char current = password.charAt(i);

            if (!Character.isWhitespace(current)) {
                builder.append(current);
            }
        }

        ByteBuffer buffer = charset.encode(CharBuffer.wrap(builder));
        byte[] bytes = new byte[buffer.remaining()];

        buffer.get(bytes);

        return bytes;
    }

    private static long firstPart(long results) {
        // The first bit must be 0, so a mask of 0x7FFF has the same result as 0xFFFF
        return (results >>> Integer.SIZE) & Integer.MAX_VALUE;
    }

    private static long secondPart(long results) {
        // The first bit must be 0, so a mask of 0x7FFF has the same result as 0xFFFF
        return results & Integer.MAX_VALUE;
    }

    /**
     * Hash the content to a 64-bits result, with the upper 32-bits being the first part and the lower 32-bits
     * being the second part.
     *
     * @param plaintext the plain text content
     * @return the 64-bits result contains 2 parts
     */
    private static long hashing(byte[] plaintext) {
        long firstPart = FIRST_HASHING;
        long secondPart = SECOND_HASHING;
        long sum = SUM_INIT_VALUE;

        for (byte current : plaintext) {
            // To unsigned byte.
            int bits = current & 0xFF;

            firstPart ^= (firstPart << Byte.SIZE) + ((firstPart & HASH_MARK) + sum) * bits;
            secondPart += firstPart ^ (secondPart << Byte.SIZE);
            sum += bits;
        }

        return ((firstPart & Integer.MAX_VALUE) << Integer.SIZE) | (secondPart & Integer.MAX_VALUE);
    }

    private OldAuthProvider() { }
}
