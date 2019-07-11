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

package io.github.mirromutth.r2dbc.mysql;

import io.netty.buffer.ByteBuf;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.require;
import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * MySQL server version, looks like {@literal "8.0.14"}.
 */
public final class ServerVersion implements Comparable<ServerVersion> {

    public static final ServerVersion NONE = new ServerVersion(0, 0, 0);

    private final int major;

    private final int minor;

    private final int patch;

    private ServerVersion(int major, int minor, int patch) {
        this.major = major;
        this.minor = minor;
        this.patch = patch;
    }

    /**
     * Parse a {@link ServerVersion} from {@link ByteBuf} encoded by ASCII.
     *
     * @param version buffer encoded by ASCII
     * @return A server version that value decode from {@code version}.
     * @throws IllegalArgumentException if {@code version} is null, or any version part overflows to a negative integer.
     */
    public static ServerVersion parse(ByteBuf version) {
        requireNonNull(version, "version must not be null");

        int major = readIntBeforePoint(version);
        int minor = readIntBeforePoint(version);
        return create(major, minor, readIntBeforePoint(version));
    }

    /**
     * Create a {@link ServerVersion} that value is {@literal major.minor.patch}.
     *
     * @param major must not be a negative integer
     * @param minor must not be a negative integer
     * @param patch must not be a negative integer
     * @return A server version that value is {@literal major.minor.patch}
     * @throws IllegalArgumentException if any version part is negative integer.
     */
    public static ServerVersion create(int major, int minor, int patch) {
        require(major >= 0, "major version must not be a negative integer");
        require(minor >= 0, "minor version must not be a negative integer");
        require(patch >= 0, "patch version must not be a negative integer");

        if (major == 0 && minor == 0 && patch == 0) {
            return NONE;
        }

        return new ServerVersion(major, minor, patch);
    }

    public int compareTo(int major, int minor, int patch) {
        if (this.major != major) {
            return (this.major < major) ? -1 : 1;
        } else if (this.minor != minor) {
            return (this.minor < minor) ? -1 : 1;
        }

        return Integer.compare(this.patch, patch);
    }

    @Override
    public int compareTo(ServerVersion version) {
        if (version == null) {
            return compareTo(NONE.major, NONE.minor, NONE.patch);
        }

        return compareTo(version.major, version.minor, version.patch);
    }

    public int getMajor() {
        return major;
    }

    public int getMinor() {
        return minor;
    }

    public int getPatch() {
        return patch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ServerVersion)) {
            return false;
        }

        ServerVersion that = (ServerVersion) o;

        if (major != that.major) {
            return false;
        }
        if (minor != that.minor) {
            return false;
        }
        return patch == that.patch;
    }

    @Override
    public int hashCode() {
        int result = major;
        result = 31 * result + minor;
        result = 31 * result + patch;
        return result;
    }

    @Override
    public String toString() {
        return String.format("%d.%d.%d", major, minor, patch);
    }

    private static int readIntBeforePoint(ByteBuf version) {
        int readerIndex = version.readerIndex();
        int result = 0;
        int digits = version.bytesBefore((byte) '.');
        int completeIndex;

        if (digits < 0) {
            // Contains not '.', read until end of buffer.
            completeIndex = version.writerIndex();

            if (completeIndex <= readerIndex) {
                // Cannot read any byte.
                return 0;
            }
        } else {
            // Read before '.', and ignore last '.'
            completeIndex = readerIndex + digits + 1;
        }

        for (int i = readerIndex; i < completeIndex; ++i) {
            byte current = version.getByte(i);

            if (current < '0' || current > '9') { // C style condition, maybe should use `!Character.isDigit(current)`?
                break;
            }

            result = result * 10 + (current - '0');
        }

        version.readerIndex(completeIndex);
        return result;
    }
}
