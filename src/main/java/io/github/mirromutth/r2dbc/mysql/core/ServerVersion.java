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

package io.github.mirromutth.r2dbc.mysql.core;

import io.netty.buffer.ByteBuf;

import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNegative;
import static io.github.mirromutth.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * MySQL server version, looks like {@code "8.0.14"}.
 */
public final class ServerVersion implements Comparable<ServerVersion> {

    private static final ServerVersion NONE = new ServerVersion(0, 0, 0);

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
        return create(major, minor, readIntBeforePoint(version));
    }

    public static ServerVersion create(int major, int minor, int patch) {
        requireNonNegative(major, "major version must not be negative");
        requireNonNegative(minor, "minor version must not be negative");
        requireNonNegative(patch, "patch version must not be negative");

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
        boolean hasPoint = true;
        int result = 0;

        if (digits < 0) { // no '.'
            hasPoint = false;
            digits = version.readableBytes(); // read until end of buffer

            if (digits < 1) { // can not read any byte
                return 0;
            }
        }

        for (int i = 0; i < digits; ++i) {
            byte current = version.readByte();

            if (current < '0' || current > '9') { // C style condition, maybe should use `!Character.isDigit(current)`?
                throw new NumberFormatException("can not parse digit " + (char) current);
            }

            result = result * 10 + (current - '0');
        }

        if (hasPoint) {
            version.skipBytes(1); // skip the '.'
        }

        return result;
    }
}
