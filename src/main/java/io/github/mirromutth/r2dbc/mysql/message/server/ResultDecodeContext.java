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

import io.github.mirromutth.r2dbc.mysql.constant.DataType;

import java.util.concurrent.atomic.AtomicInteger;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.require;

/**
 * An implementation of {@link DecodeContext} for text or binary result.
 */
final class ResultDecodeContext extends MetadataDecodeContext {

    private final boolean binary;

    private final DefinitionMetadataMessage[] metadataMessages;

    private final AtomicInteger columns = new AtomicInteger(0);

    ResultDecodeContext(boolean binary, int totalColumns) {
        require(totalColumns > 0, "result must has least 1 column");

        this.binary = binary;
        this.metadataMessages = new DefinitionMetadataMessage[totalColumns];
    }

    @Override
    public String toString() {
        return "DecodeContext-Result{binary=" + binary + "}";
    }

    public boolean isBinary() {
        return binary;
    }

    @Override
    boolean isMetadata() {
        return columns.get() < metadataMessages.length;
    }

    @Override
    DefinitionMetadataMessage[] pushAndGetMetadata(DefinitionMetadataMessage metadata) {
        int index = columns.getAndIncrement();
        int size = metadataMessages.length;

        if (index >= size) {
            throw new IllegalStateException(String.format("columns' metadata has filled up, now index: %d, array length: %d", index, size));
        }

        metadataMessages[index] = metadata;

        if (index == size - 1) {
            return metadataMessages;
        } else {
            return null;
        }
    }

    DataType getType(int index) {
        return metadataMessages[index].getType();
    }

    int getTotalColumns() {
        return metadataMessages.length;
    }
}
