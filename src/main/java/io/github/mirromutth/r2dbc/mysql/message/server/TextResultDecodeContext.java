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

import reactor.util.annotation.Nullable;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A text result decode context.
 */
final class TextResultDecodeContext implements DecodeContext {

    private final AtomicInteger columns = new AtomicInteger(0);

    private final ColumnMetadataMessage[] metadataMessages;

    TextResultDecodeContext(int totalColumns) {
        this.metadataMessages = new ColumnMetadataMessage[totalColumns];
    }

    @Override
    public DecodeContext onError() {
        return CommandDecodeContext.INSTANCE;
    }

    @Override
    public String toString() {
        return "DecodeContext-TextResult";
    }

    boolean isMetadata() {
        return columns.get() < metadataMessages.length;
    }

    @Nullable
    ColumnMetadataMessage[] pushAndGetMetadata(ColumnMetadataMessage columnMetadata) {
        int index = columns.getAndIncrement();
        int size = metadataMessages.length;

        if (index >= size) {
            throw new IllegalStateException("columns' metadata is already filled up, now index: " + index + ", array length: " + size);
        }

        metadataMessages[index] = columnMetadata;

        if (index == size - 1) {
            return metadataMessages;
        } else {
            return null;
        }
    }

    int getTotalColumns() {
        return metadataMessages.length;
    }
}
