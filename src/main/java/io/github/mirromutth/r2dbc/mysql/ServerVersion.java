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

import io.github.mirromutth.r2dbc.mysql.internal.CodecUtils;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.require;
import static io.github.mirromutth.r2dbc.mysql.internal.AssertUtils.requireNonNull;

/**
 * MySQL server version, looks like {@literal "8.0.14"}, or {@literal "8.0.14-rc2"}.
 */
public final class ServerVersion implements Comparable<ServerVersion> {

    public static final ServerVersion NONE = new ServerVersion(0, 0, 0, "");

    private static final String[] ENTERPRISES = new String[] {
        "enterprise",
        "commercial",
        "advanced"
    };

    private final int major;

    private final int minor;

    private final int patch;

    /**
     * Don't use it on {@link #hashCode()}, {@link #equals(Object)} or {@link #compareTo(ServerVersion)}.
     */
    private transient final String postfix;

    private ServerVersion(int major, int minor, int patch, String postfix) {
        this.major = major;
        this.minor = minor;
        this.patch = patch;
        this.postfix = postfix;
    }

    /**
     * Parse a {@link ServerVersion} from {@link String}.
     *
     * @param version origin version string.
     * @return A {@link ServerVersion} that value decode from {@code version}.
     * @throws IllegalArgumentException if {@code version} is null, or any version part overflows to a negative integer.
     */
    public static ServerVersion parse(ByteBuf version) {
        requireNonNull(version, "version must not be null");

        int major = CodecUtils.readIntInDigits(version, false);

        if (!version.isReadable()) {
            // End-of-buffer.
            return create0(major, 0, 0, "");
        } else if (version.getByte(version.readerIndex()) != '.') {
            // Is not '.', has only postfix after major.
            return create0(major, 0, 0, version.toString(StandardCharsets.US_ASCII));
        }

        // Skip last point '.' after read major.
        int minor = CodecUtils.readIntInDigits(version.skipBytes(1), false);

        if (!version.isReadable()) {
            return create0(major, minor, 0, "");
        } else if (version.getByte(version.readerIndex()) != '.') {
            // Is not '.', has only postfix after minor.
            return create0(major, minor, 0, version.toString(StandardCharsets.US_ASCII));
        }

        // Skip last point '.' after read minor.
        int patch = CodecUtils.readIntInDigits(version.skipBytes(1), false);

        if (!version.isReadable()) {
            // End-of-buffer, version just like X.Y.Z without any postfix.
            return create0(major, minor, patch, "");
        } else {
            // Has a postfix.
            return create0(major, minor, patch, version.toString(StandardCharsets.US_ASCII));
        }
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
        return create0(major, minor, patch, "");
    }

    /**
     * Returns whether the current {@link ServerVersion} is greater (higher, newer) or the same as the given one.
     *
     * @param version the give one
     * @return {@code true} if greater or the same as {@code version}, otherwise {@code false}
     */
    public boolean isGreaterThanOrEqualTo(ServerVersion version) {
        return compareTo(version.major, version.minor, version.patch) >= 0;
    }

    public int compareTo(int major, int minor, int patch) {
        if (this.major != major) {
            return (this.major < major) ? -1 : 1;
        } else if (this.minor != minor) {
            return (this.minor < minor) ? -1 : 1;
        } else if (this.patch != patch) {
            return (this.patch < patch) ? -1 : 1;
        }

        return 0;
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

    public String getPostfix() {
        return postfix;
    }

    public boolean isEnterprise() {
        // TODO: optimize performance using substring matching/searching algorithms, like two-way or KMP.
        for (String enterprise : ENTERPRISES) {
            // Maybe should ignore case?
            if (postfix.contains(enterprise)) {
                return true;
            }
        }

        return false;
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
        if (postfix.isEmpty()) {
            return String.format("%d.%d.%d", major, minor, patch);
        } else {
            return String.format("%d.%d.%d%s", major, minor, patch, postfix);
        }
    }

    private static ServerVersion create0(int major, int minor, int patch, String postfix) {
        require(major >= 0, "major version must not be a negative integer");
        require(minor >= 0, "minor version must not be a negative integer");
        require(patch >= 0, "patch version must not be a negative integer");

        if (major == 0 && minor == 0 && patch == 0) {
            return NONE;
        }

        return new ServerVersion(major, minor, patch, postfix);
    }
}
