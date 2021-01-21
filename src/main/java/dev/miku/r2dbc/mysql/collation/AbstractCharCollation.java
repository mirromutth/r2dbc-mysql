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

package dev.miku.r2dbc.mysql.collation;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An abstraction of {@link CharCollation}.
 */
abstract class AbstractCharCollation implements CharCollation {

    protected final int id;

    protected final String name;

    final CharsetTarget target;

    AbstractCharCollation(int id, String name, CharsetTarget target) {
        this.id = id;
        this.name = requireNonNull(name, "name must not be null");
        this.target = requireNonNull(target, "target must not be null");
    }

    @Override
    public final int getId() {
        return id;
    }

    @Override
    public final String getName() {
        return name;
    }

    @Override
    public int getByteSize() {
        return target.getByteSize();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AbstractCharCollation)) {
            return false;
        }

        AbstractCharCollation that = (AbstractCharCollation) o;

        return id == that.id && name.equals(that.name) && target.equals(that.target);
    }

    @Override
    public int hashCode() {
        int hash = 31 * id + name.hashCode();
        return 31 * hash + target.hashCode();
    }
}
