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

package dev.miku.r2dbc.mysql.authentication;

import dev.miku.r2dbc.mysql.collation.CharCollation;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static dev.miku.r2dbc.mysql.constant.DataValues.TERMINAL;

/**
 * A utility for general authentication hashing algorithm.
 */
final class AuthHelper {

    private AuthHelper() {
    }

    static byte[] generalHash(String algorithm, boolean leftSalt, CharSequence password, byte[] salt, CharCollation collation) {
        Charset charset = collation.getCharset();
        MessageDigest digest = loadDigest(algorithm);

        byte[] oneRound = digestBuffer(digest, charset.encode(CharBuffer.wrap(password)));
        byte[] twoRounds = finalDigests(digest, oneRound);

        if (leftSalt) {
            return allBytesXor(finalDigests(digest, salt, twoRounds), oneRound);
        } else {
            return allBytesXor(finalDigests(digest, twoRounds, salt), oneRound);
        }
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
            throw new IllegalArgumentException(name + " not support of MessageDigest", e);
        }
    }

    private static byte[] finalDigests(MessageDigest digest, byte[]... plains) {
        digest.reset();

        for (byte[] plain : plains) {
            digest.update(plain);
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
            throw new IllegalArgumentException("can not xor different sizes " + size + " and " + right.length);
        }

        for (int i = 0; i < size; ++i) {
            left[i] ^= right[i];
        }

        return left;
    }
}
