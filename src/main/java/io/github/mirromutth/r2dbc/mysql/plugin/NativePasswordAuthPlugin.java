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

package io.github.mirromutth.r2dbc.mysql.plugin;

import io.github.mirromutth.r2dbc.mysql.constant.AuthType;
import reactor.util.annotation.Nullable;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static java.util.Objects.requireNonNull;

/**
 * MySQL Authentication Plugin for "mysql_native_password"
 */
public class NativePasswordAuthPlugin implements AuthPlugin {

    private final static NativePasswordAuthPlugin INSTANCE = new NativePasswordAuthPlugin();
    private final static byte[] EMPTY_BYTES = new byte[0];

    private NativePasswordAuthPlugin() {}

    public static NativePasswordAuthPlugin getInstance() {
        return INSTANCE;
    }

    @Override
    public AuthType getType() {
        return AuthType.MYSQL_NATIVE_PASSWORD;
    }

    /**
     * SHA1(password) all bytes xor SHA1( "random data from MySQL server" + SHA1(SHA1(password)) )
     *
     * @param password plaintext password
     * @param seed 20 bytes random seed from MySQL server
     * @return encrypted authentication password
     */
    @Override
    public byte[] encrypt(@Nullable byte[] password, byte[] seed) {
        if (password == null) {
            return EMPTY_BYTES;
        }

        requireNonNull(seed);

        MessageDigest digest = newSha1Digest();
        byte[] oneRound = finalDigests(digest, password);
        byte[] twoRounds = finalDigests(digest, oneRound);
        byte[] result = finalDigests(digest, seed, twoRounds);

        for (int i = 0; i < result.length; ++i) {
            result[i] ^= oneRound[i];
        }

        return result;
    }

    private byte[] finalDigests(MessageDigest digest, byte[] ...plains) {
        for (byte[] plain : plains) {
            digest.update(plain);
        }

        byte[] result = digest.digest();
        digest.reset();
        return result;
    }

    private MessageDigest newSha1Digest() {
        try {
            return MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
