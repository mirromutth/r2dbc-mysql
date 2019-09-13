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

package dev.miku.r2dbc.mysql.message;

import dev.miku.r2dbc.mysql.internal.AssertUtils;
import io.netty.buffer.ByteBuf;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;

/**
 * An implementation of {@link FieldValue} considers field value bytes is less or equals than {@link Integer#MAX_VALUE}.
 */
public final class NormalFieldValue extends AbstractReferenceCounted implements FieldValue {

    private final ByteBuf buf;

    public NormalFieldValue(ByteBuf buf) {
        this.buf = AssertUtils.requireNonNull(buf, "buf must not be null");
    }

    public ByteBuf getBufferSlice() {
        return buf.slice();
    }

    @Override
    public ReferenceCounted touch(Object hint) {
        return buf.touch(hint);
    }

    @Override
    protected void deallocate() {
        buf.release();
    }
}
