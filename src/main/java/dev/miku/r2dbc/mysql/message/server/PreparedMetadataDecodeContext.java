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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * An implementation of {@link DecodeContext} for prepared metadata.
 */
final class PreparedMetadataDecodeContext extends MetadataDecodeContext {

    /**
     * First part of metadata which is parameters metadata.
     */
    private final DefinitionMetadataMessage[] paramMetadata;

    /**
     * Second part of metadata which is columns metadata.
     */
    private final DefinitionMetadataMessage[] colMetadata;

    private final AtomicInteger columns = new AtomicInteger();

    private volatile boolean inMetadata = true;

    PreparedMetadataDecodeContext(boolean eofDeprecated, int totalColumns, int totalParameters) {
        super(eofDeprecated);
        this.paramMetadata = createArray(totalParameters);
        this.colMetadata = createArray(totalColumns);
    }

    @Override
    public String toString() {
        return "DecodeContext-PreparedMetadata";
    }

    @Override
    boolean isInMetadata() {
        return inMetadata;
    }

    @Override
    protected SyntheticMetadataMessage checkComplete(int index, @Nullable EofMessage eof) {
        if (index == paramMetadata.length) {
            if (colMetadata.length == 0) {
                // Has no column metadata.
                inMetadata = false;
                return new SyntheticMetadataMessage(true, paramMetadata, eof);
            }

            return new SyntheticMetadataMessage(false, paramMetadata, eof);
        } else if (index == paramMetadata.length + colMetadata.length) {
            inMetadata = false;
            return new SyntheticMetadataMessage(true, colMetadata, eof);
        } else {
            return null;
        }
    }

    @Override
    protected int putMetadata(DefinitionMetadataMessage metadata) {
        int index = columns.getAndIncrement();
        int paramSize = paramMetadata.length;
        int colSize = colMetadata.length;

        if (index >= paramSize + colSize) {
            throw new IllegalStateException("Columns metadata has filled up, now index: " + index +
                ", param length: " + paramSize + ", column length: " + colSize);
        }

        if (index < paramSize) {
            paramMetadata[index] = metadata;
        } else {
            colMetadata[index - paramSize] = metadata;
        }

        return index + 1;
    }

    @Override
    protected int currentIndex() {
        return columns.get();
    }

    @Override
    protected Object loggingPoints() {
        int paramSize = paramMetadata.length;
        return "[" + paramSize + ", " + (paramSize + colMetadata.length) + ']';
    }

    private static DefinitionMetadataMessage[] createArray(int size) {
        return size == 0 ? SyntheticMetadataMessage.EMPTY_METADATA : new DefinitionMetadataMessage[size];
    }
}
