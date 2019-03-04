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

package io.github.mirromutth.r2dbc.mysql.security;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Generic crypto logic for all authentication types.
 */
final class AuthCrypto {

    private AuthCrypto() {
    }

    static byte[] usualHash(String algorithm, byte[] password, byte[] salt, boolean leftSalt) {
        MessageDigest digest = loadDigest(algorithm);
        byte[] oneRound = finalDigests(digest, password);
        byte[] twoRounds = finalDigests(digest, oneRound);

        if (leftSalt) {
            return allBytesXor(finalDigests(digest, salt, twoRounds), oneRound);
        } else {
            return allBytesXor(finalDigests(digest, twoRounds, salt), oneRound);
        }
    }

    private static MessageDigest loadDigest(String name) {
        try {
            return MessageDigest.getInstance(name);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException(name + " not support of MessageDigest", e);
        }
    }

    private static byte[] finalDigests(MessageDigest digest, byte[]... plains) {
        for (byte[] plain : plains) {
            digest.update(plain);
        }

        byte[] result = digest.digest();
        digest.reset();
        return result;
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
