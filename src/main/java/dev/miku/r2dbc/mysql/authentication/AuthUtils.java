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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static dev.miku.r2dbc.mysql.constant.Envelopes.TERMINAL;

/**
 * An utility for general authentication hashing algorithm.
 */
final class AuthUtils {

    /**
     * Common hashing process of "caching_sha2_password" and "mysql_native_password".
     *
     * <ul><li>The hashing process is HASH(plain) `all bytes xor` HASH( salt + HASH( HASH(plain) ) ) if the
     * {@code left} is {@code true}.</li>
     * <li>Otherwise, the process is HASH(plain) `all bytes xor` HASH( HASH( HASH(plain) ) + salt )</li></ul>
     * The {@code HASH} is the basic hash algorithm provided by {@link MessageDigest}.
     *
     * @param algorithm the name of the basic hash algorithm
     * @param left      load salt as left challenge
     * @param plain     plain text for hash
     * @param salt      the salt of challenge
     * @param charset   encoding {@code plain} as byte array
     * @return hash result
     * @throws IllegalArgumentException if {@code algorithm} not found
     */
    static byte[] hash(String algorithm, boolean left, CharSequence plain, byte[] salt, Charset charset) {
        MessageDigest digest = loadDigest(algorithm);

        byte[] oneRound = digestBuffer(digest, charset.encode(CharBuffer.wrap(plain)));
        byte[] twoRounds = finalDigests(digest, oneRound);

        return allBytesXor(finalDigests(digest, left, salt, twoRounds), oneRound);
    }

    static byte[] encodeTerminal(CharBuffer chars, Charset charset) {
        ByteBuffer buffer = charset.encode(chars);
        int maxIndex = buffer.remaining();
        byte[] bytes = new byte[maxIndex + 1];

        buffer.get(bytes, 0, maxIndex);
        bytes[maxIndex] = TERMINAL;

        return bytes;
    }

    private static MessageDigest loadDigest(String name) {
        try {
            return MessageDigest.getInstance(name);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException(e.getLocalizedMessage(), e);
        }
    }

    private static byte[] finalDigests(MessageDigest digest, byte[] plain) {
        digest.reset();
        digest.update(plain);

        return digest.digest();
    }

    private static byte[] finalDigests(MessageDigest digest, boolean leftFirst, byte[] left, byte[] right) {
        digest.reset();

        if (leftFirst) {
            digest.update(left);
            digest.update(right);
        } else {
            digest.update(right);
            digest.update(left);
        }

        return digest.digest();
    }

    private static byte[] digestBuffer(MessageDigest digest, ByteBuffer buffer) {
        digest.update(buffer);
        return digest.digest();
    }

    private static byte[] allBytesXor(byte[] left, byte[] right) {
        int size = left.length;

        if (size != right.length) {
            throw new IllegalArgumentException("Cannot xor different sizes " + size + " and " + right.length);
        }

        for (int i = 0; i < size; ++i) {
            left[i] ^= right[i];
        }

        return left;
    }

    private AuthUtils() { }
}
