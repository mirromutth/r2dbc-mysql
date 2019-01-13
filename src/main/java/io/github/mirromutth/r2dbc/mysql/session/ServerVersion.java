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

package io.github.mirromutth.r2dbc.mysql.session;

import io.netty.buffer.ByteBuf;

import static java.util.Objects.requireNonNull;

/**
 * MySQL server version, looks like {@code "8.0.14"}.
 */
public final class ServerVersion implements Comparable<ServerVersion> {

    private final int major;

    private final int minor;

    private final int patch;

    private ServerVersion(int major, int minor, int patch) {
        this.major = major;
        this.minor = minor;
        this.patch = patch;
    }

    public static ServerVersion parse(ByteBuf version) {
        requireNonNull(version, "version must not be null");

        int major = readIntBeforePoint(version);
        int minor = readIntBeforePoint(version);
        return new ServerVersion(major, minor, readIntBeforePoint(version));
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
        requireNonNull(version, "version must not be null");
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
        return "MySQL<" + major + '.' + minor + '.' + patch + '>';
    }

    private static int readIntBeforePoint(ByteBuf version) {
        int digits = version.bytesBefore((byte) '.');
        boolean hasNext = true;
        int result = 0;

        if (digits < 0) {
            digits = version.readableBytes(); // read until end of buffer

            if (digits < 1) { // can not read any byte
                return 0;
            }

            hasNext = false;
        }

        for (int i = 0; i < digits; ++i) {
            byte current = version.readByte();

            if (current < '0' || current > '9') {
                throw new NumberFormatException("can not parse digit " + (char) current);
            }

            result = result * 10 + (current - '0');
        }

        if (hasNext) {
            version.skipBytes(1);
        }

        return result;
    }
}
