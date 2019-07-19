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

package io.github.mirromutth.r2dbc.mysql.message.header;

/**
 * An implementation of {@link SequenceIdProvider} based on unsafe increment.
 * <p>
 * It is NOT thread-safety, used only for temporary use of a single message.
 */
final class UnsafeSequenceIdProvider implements SequenceIdProvider {

    private int i = 0;

    @Override
    public byte next() {
        return (byte) ((i++) & 0xFF);
    }
}
