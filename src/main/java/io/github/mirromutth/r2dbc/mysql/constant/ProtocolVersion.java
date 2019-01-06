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

package io.github.mirromutth.r2dbc.mysql.constant;

/**
 * Protocol versions for MySQL Protocol, not support version 9.
 * <p>
 * Can NOT promise the {@link #name()} always equals than {@code "V$code"}
 */
public enum ProtocolVersion {

    V10(10);

    private final int code;

    ProtocolVersion(int code) {
        this.code = code;
    }

    /**
     * Do NOT use it outer than {@code r2dbc-mysql}, because it is native flag code of MySQL,
     * we can NOT promise it will be never changes.
     *
     * @return the native flag code
     */
    public int getCode() {
        return code;
    }
}
