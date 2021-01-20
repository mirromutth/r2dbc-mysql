/*
 * Copyright 2018-2020 the original author or authors.
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

package dev.miku.r2dbc.mysql.util;

import dev.miku.r2dbc.mysql.message.FieldValue;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;

import java.util.List;

/**
 * An internal utility considers the use of safe release buffers (array or {@link List}). It uses standard
 * netty {@link ReferenceCountUtil#safeRelease} to suppress release errors.
 */
public final class NettyBufferUtils {

    public static void releaseAll(ReferenceCounted[] parts) {
        for (ReferenceCounted counted : parts) {
            if (counted != null) {
                ReferenceCountUtil.safeRelease(counted);
            }
        }
    }

    public static void releaseAll(FieldValue[] fields) {
        for (FieldValue field : fields) {
            if (field != null && !field.isNull()) {
                ReferenceCountUtil.safeRelease(field);
            }
        }
    }

    public static void releaseAll(List<? extends ReferenceCounted> parts) {
        release0(parts, parts.size());
    }

    public static void releaseAll(FieldValue[] fields, int bound) {
        int size = Math.min(bound, fields.length);

        for (int i = 0; i < size; ++i) {
            FieldValue field = fields[i];
            if (field != null && !field.isNull()) {
                ReferenceCountUtil.safeRelease(field);
            }
        }
    }

    public static void releaseAll(List<? extends ReferenceCounted> parts, int bound) {
        release0(parts, Math.min(bound, parts.size()));
    }

    private static void release0(List<? extends ReferenceCounted> parts, int size) {
        for (int i = 0; i < size; ++i) {
            ReferenceCounted counted = parts.get(i);
            if (counted != null) {
                ReferenceCountUtil.safeRelease(counted);
            }
        }
    }

    private NettyBufferUtils() { }
}
