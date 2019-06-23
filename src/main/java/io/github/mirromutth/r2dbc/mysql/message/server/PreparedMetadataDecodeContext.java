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
final class PreparedMetadataDecodeContext extends MetadataDecodeContext implements CompletableDecodeContext {

    private static final DefinitionMetadataMessage[] EMPTY_METADATA = {};

    private final DefinitionMetadataMessage[] paramMetadata;

    private final DefinitionMetadataMessage[] colMetadata;

    private final AtomicInteger columns = new AtomicInteger(0);

    PreparedMetadataDecodeContext(int totalColumns, int totalParameters) {
        this.paramMetadata = createArray(totalParameters);
        this.colMetadata = createArray(totalColumns);
    }

    @Override
    public boolean isCompleted() {
        return !isMetadata();
    }

    @Override
    public DecodeContext nextContext() {
        return CommandDecodeContext.INSTANCE;
    }

    @Override
    public ServerMessage fakeMessage() {
        return FakePrepareCompleteMessage.INSTANCE;
    }

    @Override
    public String toString() {
        return "DecodeContext-PreparedMetadata";
    }

    @Override
    boolean isMetadata() {
        return columns.get() < paramMetadata.length + colMetadata.length;
    }

    @Override
    DefinitionMetadataMessage[] pushAndGetMetadata(DefinitionMetadataMessage metadata) {
        int columns = this.columns.getAndIncrement();
        int paramSize = paramMetadata.length;
        int colSize = colMetadata.length;

        if (columns >= paramSize + colSize) {
            throw new IllegalStateException(String.format("columns' metadata has filled up, now index: %d, param length: %d, column length: %d", columns, paramSize, colSize));
        }

        if (columns < paramSize) {
            paramMetadata[columns] = metadata;

            if (columns == paramSize - 1) {
                return paramMetadata;
            }

            return null;
        }

        colMetadata[columns -= paramSize] = metadata;

        if (columns == colSize - 1) {
            return colMetadata;
        }

        return null;
    }

    private static DefinitionMetadataMessage[] createArray(int size) {
        if (size > 0) {
            return new DefinitionMetadataMessage[size];
        } else {
            return EMPTY_METADATA;
        }
    }
}
