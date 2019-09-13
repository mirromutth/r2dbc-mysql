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

import dev.miku.r2dbc.mysql.internal.AssertUtils;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * An implementation of {@link DecodeContext} for text or binary result.
 */
final class ResultDecodeContext extends MetadataDecodeContext {

    private final DefinitionMetadataMessage[] metadataMessages;

    private final AtomicInteger columns = new AtomicInteger();

    private boolean inMetadata = true;

    ResultDecodeContext(boolean deprecateEof, int totalColumns) {
        super(deprecateEof);

        AssertUtils.require(totalColumns > 0, "result must has least 1 column");

        this.metadataMessages = new DefinitionMetadataMessage[totalColumns];
    }

    @Override
    public String toString() {
        return "DecodeContext-Result";
    }

    @Override
    boolean isInMetadata() {
        return inMetadata;
    }

    @Override
    protected SyntheticMetadataMessage checkComplete(int index) {
        if (index == metadataMessages.length) {
            inMetadata = false;

            // In results, row metadata has filled-up does not means complete. (has rows or OK/EOF following)
            return new SyntheticMetadataMessage(false, metadataMessages);
        } else {
            return null;
        }
    }

    @Override
    protected int putMetadata(DefinitionMetadataMessage metadata) {
        int index = columns.getAndIncrement();
        int size = metadataMessages.length;

        if (index >= size) {
            throw new IllegalStateException(String.format("columns' metadata has filled up, now index: %d, array length: %d", index, size));
        }

        metadataMessages[index] = metadata;

        return index + 1;
    }

    @Override
    protected int currentIndex() {
        return columns.get();
    }

    @Override
    protected Object loggingPoints() {
        return metadataMessages.length;
    }

    short getType(int index) {
        return metadataMessages[index].getType();
    }

    int getTotalColumns() {
        return metadataMessages.length;
    }
}
