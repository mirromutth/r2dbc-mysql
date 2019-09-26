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

package dev.miku.r2dbc.mysql.message.server;

import java.util.Arrays;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * A message contains a bundle of {@link DefinitionMetadataMessage}s, {@link #isCompleted()} returning
 * {@code true} means it is last metadata bundle of current query.
 * <p>
 * Note: all subclasses are synthetic messages, not real exists.
 */
public final class SyntheticMetadataMessage implements ServerMessage {

    private final boolean completed;

    private final DefinitionMetadataMessage[] messages;

    SyntheticMetadataMessage(boolean completed, DefinitionMetadataMessage[] messages) {
        this.completed = completed;
        this.messages = requireNonNull(messages, "messages must not be null");
    }

    public final DefinitionMetadataMessage[] unwrap() {
        return messages;
    }

    public final boolean isCompleted() {
        return completed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SyntheticMetadataMessage)) {
            return false;
        }

        SyntheticMetadataMessage that = (SyntheticMetadataMessage) o;

        if (completed != that.completed) {
            return false;
        }
        return Arrays.equals(messages, that.messages);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(messages);
        result = 31 * result + (completed ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        if (messages.length <= 3) {
            return String.format("SyntheticMetadataMessage{completed=%b, messages=%s}", completed, Arrays.toString(messages));
        }

        // MySQL support 4096 columns for pre-table, no need print large bundle of messages in here.
        return String.format("SyntheticMetadataMessage{completed=%b, messages=[%s, %s, ...more %d messages]}", completed, messages[0], messages[1], messages.length - 2);
    }
}
