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

package io.github.mirromutth.r2dbc.mysql.message.server;

import java.util.Arrays;
import java.util.Objects;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * A virtual message that is a bundle of {@link ColumnMetadataMessage}s.
 */
public final class FictitiousRowMetadataMessage implements ServerMessage {

    private final ColumnMetadataMessage[] messages;

    FictitiousRowMetadataMessage(ColumnMetadataMessage[] messages) {
        this.messages = requireNonNull(messages, "messages must not be null");
    }

    public ColumnMetadataMessage[] getMessages() {
        return messages;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FictitiousRowMetadataMessage)) {
            return false;
        }

        FictitiousRowMetadataMessage that = (FictitiousRowMetadataMessage) o;

        int size = messages.length;

        if (size != that.messages.length) {
            return false;
        }

        for (int i = 0; i < size; ++i) {
            if (!Objects.equals(messages[i], that.messages[i])) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(messages);
    }

    @Override
    public String toString() {
        return "FictitiousRowMetadataMessage{" +
            "messages=" + Arrays.toString(messages) +
            '}';
    }
}
