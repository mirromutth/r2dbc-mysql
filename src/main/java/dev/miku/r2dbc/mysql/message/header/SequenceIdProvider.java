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

package dev.miku.r2dbc.mysql.message.header;

/**
 * Provider for the SequenceId header field.
 */
public interface SequenceIdProvider {

    /**
     * @return the next sequence id.
     */
    byte next();

    /**
     * A {@link SequenceIdProvider} considers linking last envelope.
     */
    interface Linkable extends SequenceIdProvider {

        /**
         * Set the last sequence id is {@code value}.
         *
         * @param value last sequence id.
         */
        void last(int value);
    }

    /**
     * Atomic/concurrent sequence id provider.
     *
     * @return a thread-safe packet counter.
     */
    static Linkable atomic() {
        return new AtomicSequenceIdProvider();
    }

    static SequenceIdProvider unsafe() {
        return new UnsafeSequenceIdProvider();
    }
}
