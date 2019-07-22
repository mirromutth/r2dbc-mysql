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

package io.github.mirromutth.r2dbc.mysql.authentication;

import io.github.mirromutth.r2dbc.mysql.collation.CharCollation;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;

import static io.github.mirromutth.r2dbc.mysql.constant.AuthTypes.MYSQL_OLD_PASSWORD;
import static io.github.mirromutth.r2dbc.mysql.constant.EmptyArrays.EMPTY_BYTES;

/**
 * An implementation of {@link MySqlAuthProvider} for type "mysql_old_password".
 * <p>
 * WARNING: The hashing algorithm has broken that is used for the Old Password Authentication
 * "mysql_old_password" (as shown in CVE-2000-0981).
 */
final class OldAuthProvider implements MySqlAuthProvider {

    static final OldAuthProvider INSTANCE = new OldAuthProvider();

    private static final long FIRST_HASHING = 0x50305735L;

    private static final long SECOND_HASHING = 0x12345671L;

    private static final int SUM_INIT_VALUE = 7;

    private static final int HASH_MARK = 0x3F;

    private static final long MOD = 0x3FFFFFFFL;

    private static final int SEED_INC = 0x21;

    private static final int SEED_MULTIPLIER = 3;

    private static final int RESULT_INC = 0x40;

    private static final int RESULT_MULTIPLIER = 0x1F;

    private OldAuthProvider() {
    }

    @Override
    public String getType() {
        return MYSQL_OLD_PASSWORD;
    }

    @Override
    public boolean isSslNecessary() {
        return false;
    }

    @Override
    public byte[] fastAuthPhase(@Nullable CharSequence password, @Nullable byte[] salt, CharCollation collation) {
        if (password == null || password.length() <= 0) {
            return EMPTY_BYTES;
        }

        Charset charset = collation.getCharset();
        String saltString;

        if (salt == null) {
            saltString = "";
            salt = EMPTY_BYTES;
        } else {
            saltString = new String(salt, charset);
        }

        // Authentication results
        long authentications = hashing(salt);

        // Messages results
        long messages = hashing(encodePurely(password, charset));

        long firstSeed = (firstPart(authentications) ^ firstPart(messages)) % MOD;
        long secondSeed = (secondPart(authentications) ^ secondPart(messages)) % MOD;
        int stringSize = saltString.length();
        char[] results = new char[stringSize];

        for (int i = 0; i < stringSize; ++i) {
            firstSeed = ((firstSeed * SEED_MULTIPLIER) + secondSeed) % MOD;
            secondSeed = (firstSeed + secondSeed + SEED_INC) % MOD;
            results[i] = (char) (byte) Math.floor(((((double) firstSeed) / MOD) * RESULT_MULTIPLIER) + RESULT_INC);
        }

        long lastSeed = ((firstSeed * SEED_MULTIPLIER) + secondSeed) % MOD;
        char mark = (char) (byte) Math.floor((((double) lastSeed) / MOD) * RESULT_MULTIPLIER);

        for (int i = 0; i < stringSize; i++) {
            results[i] ^= mark;
        }

        // Maybe without creating String? but it must be better than copy ByteBuffer:
        // copyToByteArray(charset.encode(CharBuffer.wrap(results)))
        return new String(results).getBytes(charset);
    }

    @Override
    public byte[] fullAuthPhase(@Nullable CharSequence password, CharCollation collation) {
        // "mysql_old_password" does not support full authentication.
        return null;
    }

    private static byte[] encodePurely(CharSequence password, Charset charset) {
        // Encoding without whitespace, so call it "purely".
        int size = password.length();
        StringBuilder builder = new StringBuilder(size);

        for (int i = 0; i < size; ++i) {
            char current = password.charAt(i);

            if (!Character.isWhitespace(current)) {
                builder.append(current);
            }
        }

        return builder.toString().getBytes(charset);
    }

    private static long firstPart(long results) {
        // First bit must be 0, so mark with 0x7FFF... same as mark with 0xFFF...
        return (results >>> Integer.SIZE) & Integer.MAX_VALUE;
    }

    private static long secondPart(long results) {
        // First bit must be 0, so mark with 0x7FFF... same as mark with 0xFFF...
        return results & Integer.MAX_VALUE;
    }

    /**
     * @return pairs of hashing results, high 32-bits is first part, low 32-bits is second part.
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
}
