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

import io.github.mirromutth.r2dbc.mysql.message.FieldValue;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;

import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * Base class considers fields getter for {@link RowMessage} implementations.
 */
abstract class AbstractRowMessage extends AbstractReferenceCounted implements RowMessage {

    private final FieldValue[] fields;

    AbstractRowMessage(FieldValue[] fields) {
        this.fields = requireNonNull(fields, "fields must not be null");
    }

    @Override
    public final FieldValue[] getFields() {
        return fields;
    }

    @Override
    public final ReferenceCounted touch(Object o) {
        for (FieldValue field : fields) {
            field.touch(o);
        }

        return this;
    }

    final int fieldsCount() {
        return fields.length;
    }

    @Override
    protected final void deallocate() {
        for (FieldValue field : fields) {
            ReferenceCountUtil.safeRelease(field);
        }
    }
}
