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
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;

import java.util.List;

/**
 * An implementation of {@link FieldValue} considers large field value which bytes more
 * than {@link Integer#MAX_VALUE}, it would be exists when MySQL server return LOB
 * types (i.e. BLOB, CLOB), LONGTEXT length can be unsigned int32.
 *
 * @see FieldValue
 */
public final class LargeFieldValue extends AbstractReferenceCounted implements FieldValue {

    private final List<ByteBuf> buffers;

    public LargeFieldValue(List<ByteBuf> buffers) {
        this.buffers = AssertUtils.requireNonNull(buffers, "buffers must not be null");
    }

    public ByteBuf[] getBufferSlices() {
        int size = this.buffers.size();
        ByteBuf[] buffers = new ByteBuf[size];

        for (int i = 0; i < size; ++i) {
            buffers[i] = this.buffers.get(i).slice();
        }

        return buffers;
    }

    @Override
    public ReferenceCounted touch(Object hint) {
        if (this.buffers.isEmpty()) {
            return this;
        }

        for (ByteBuf buf : this.buffers) {
            buf.touch(hint);
        }

        return this;
    }

    @Override
    protected void deallocate() {
        if (this.buffers.isEmpty()) {
            return;
        }

        for (ByteBuf buf : this.buffers) {
            ReferenceCountUtil.safeRelease(buf);
        }
    }
}
