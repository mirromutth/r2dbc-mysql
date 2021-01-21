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

package dev.miku.r2dbc.mysql.message.server;

import reactor.util.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * A message contains a bundle of {@link DefinitionMetadataMessage}s, when the {@link #isCompleted()}
 * returning {@code true} means it is last metadata bundle of current query.
 * <p>
 * Note: all subclasses are synthetic messages, not real exists.
 */
public final class SyntheticMetadataMessage implements ServerMessage {

    static final DefinitionMetadataMessage[] EMPTY_METADATA = { };

    private final boolean completed;

    private final DefinitionMetadataMessage[] messages;

    @Nullable
    private final EofMessage eof;

    SyntheticMetadataMessage(boolean completed, DefinitionMetadataMessage[] messages,
        @Nullable EofMessage eof) {
        this.completed = completed;
        this.messages = requireNonNull(messages, "messages must not be null");
        this.eof = eof;
    }

    public final DefinitionMetadataMessage[] unwrap() {
        return messages;
    }

    public final boolean isCompleted() {
        return completed;
    }

    @Nullable
    public EofMessage getEof() {
        return eof;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SyntheticMetadataMessage that = (SyntheticMetadataMessage) o;

        if (completed != that.completed) {
            return false;
        }
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(messages, that.messages)) {
            return false;
        }
        return Objects.equals(eof, that.eof);
    }

    @Override
    public int hashCode() {
        int result = (completed ? 1 : 0);
        result = 31 * result + Arrays.hashCode(messages);
        result = 31 * result + Objects.hashCode(eof);
        return result;
    }

    @Override
    public String toString() {
        if (messages.length <= 3) {
            return "SyntheticMetadataMessage{completed=" + completed + ", messages=" +
                Arrays.toString(messages) + ", eof=" + eof + '}';
        }

        // MySQL support 4096 columns for pre-table, no need print large bundle of messages in here.
        return "SyntheticMetadataMessage{completed=" + completed + ", messages=[" + messages[0] + ", " +
            messages[1] + ", ...more " + (messages.length - 2) + " messages], eof=" + eof + '}';
    }
}
