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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * An implementation of {@link DecodeContext} for prepared metadata.
 */
final class PreparedMetadataDecodeContext extends MetadataDecodeContext {

    private static final DefinitionMetadataMessage[] EMPTY_METADATA = {};

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

    PreparedMetadataDecodeContext(boolean deprecateEof, int totalColumns, int totalParameters) {
        super(deprecateEof);
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
    protected SyntheticMetadataMessage checkComplete(int index) {
        if (index == paramMetadata.length) {
            if (colMetadata.length == 0) {
                // Has no column metadata.
                inMetadata = false;
                return new SyntheticMetadataMessage(true, paramMetadata);
            }

            return new SyntheticMetadataMessage(false, paramMetadata);
        } else if (index == paramMetadata.length + colMetadata.length) {
            inMetadata = false;
            return new SyntheticMetadataMessage(true, colMetadata);
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
            throw new IllegalStateException(String.format("columns' metadata has filled up, now index: %d, param length: %d, column length: %d", index, paramSize, colSize));
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
        return String.format("[%d, %d]", paramSize, paramSize + colMetadata.length);
    }

    private static DefinitionMetadataMessage[] createArray(int size) {
        if (size > 0) {
            return new DefinitionMetadataMessage[size];
        } else {
            return EMPTY_METADATA;
        }
    }
}
