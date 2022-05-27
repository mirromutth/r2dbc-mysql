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

package dev.miku.r2dbc.mysql;

import dev.miku.r2dbc.mysql.extension.Extension;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ServiceLoader;
import java.util.function.Consumer;

/**
 * Utility to load and hold {@link Extension}s.
 */
final class Extensions {

    private static final Logger logger = Loggers.getLogger(Extensions.class);

    private static final Extension[] EMPTY = { };

    private final Extension[] extensions;

    private Extensions(List<Extension> extensions) {
        this.extensions = toArray(extensions);
    }

    /**
     * Apply {@link Consumer consumer} for each {@link Extension} of the requested type {@code type} until all
     * {@link Extension extensions} have been processed or the action throws an exception. Actions are
     * performed in the order of iteration, if that order is specified. Exceptions thrown by the action are
     * relayed to the caller.
     *
     * @param type     the extension type to filter for
     * @param consumer the {@link Consumer} to notify for each instance of {@code type}
     * @param <T>      extension type
     */
    <T extends Extension> void forEach(Class<T> type, Consumer<? super T> consumer) {
        for (Extension extension : this.extensions) {
            if (type.isInstance(extension)) {
                consumer.accept(type.cast(extension));
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Extensions that = (Extensions) o;
        return Arrays.equals(extensions, that.extensions);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(extensions);
    }

    @Override
    public String toString() {
        // Extensions[AbcExtension{...}, DefExtension{...}, ...]
        return "Extensions" + Arrays.toString(extensions);
    }

    /**
     * Creates a new {@link Extensions} object.
     *
     * @param extensions the extensions to hold.
     * @param autodetect use autodetect or not.
     * @return a new {@link Extensions} object.
     */
    static Extensions from(List<Extension> extensions, boolean autodetect) {
        if (autodetect) {
            return new Extensions(autodetect(new ArrayList<>(extensions)));
        }

        return new Extensions(extensions);
    }

    /**
     * Autodetect extensions using {@link ServiceLoader} mechanism.
     *
     * @param discovered existed {@link Extension}s.
     * @return the detected {@link Extension}s.
     */
    private static List<Extension> autodetect(List<Extension> discovered) {
        logger.debug("Discovering Extensions using ServiceLoader");

        ServiceLoader<Extension> extensions = ServiceLoader.load(
            Extension.class, Extensions.class.getClassLoader());

        for (Extension extension : extensions) {
            logger.debug(String.format("Registering extension %s", extension.getClass().getName()));
            discovered.add(extension);
        }

        return discovered;
    }

    private static Extension[] toArray(List<Extension> extensions) {
        if (extensions.isEmpty()) {
            return EMPTY;
        }
        return extensions.toArray(new Extension[0]);
    }
}
