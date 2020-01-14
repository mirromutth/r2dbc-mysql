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

import static dev.miku.r2dbc.mysql.util.AssertUtils.require;
import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * MySQL server version, looks like {@literal "8.0.14"}, or {@literal "8.0.14-rc2"}.
 */
public final class ServerVersion implements Comparable<ServerVersion> {

    private static final String[] ENTERPRISES = new String[]{
        "enterprise",
        "commercial",
        "advanced"
    };

    /**
     * Unresolved version string, do NOT use it on {@link #hashCode()},
     * {@link #equals(Object)} or {@link #compareTo(ServerVersion)}.
     */
    private transient final String origin;

    private final int major;

    private final int minor;

    private final int patch;

    private ServerVersion(String origin, int major, int minor, int patch) {
        this.origin = origin;
        this.major = major;
        this.minor = minor;
        this.patch = patch;
    }

    /**
     * Returns whether the current {@link ServerVersion} is greater (higher, newer) or the same as the given one.
     *
     * @param version the give one
     * @return {@code true} if greater or the same as {@code version}, otherwise {@code false}
     */
    public boolean isGreaterThanOrEqualTo(ServerVersion version) {
        return compareTo(version) >= 0;
    }

    public boolean isLessThan(ServerVersion version) {
        return compareTo(version) < 0;
    }

    @Override
    public int compareTo(ServerVersion version) {
        // Standard `Comparable` must throw `NullPointerException` in `compareTo`,
        // so cannot use `AssertUtils.requireNonNull` in here (throws `IllegalArgumentException`).

        if (this.major != version.major) {
            return (this.major < version.major) ? -1 : 1;
        } else if (this.minor != version.minor) {
            return (this.minor < version.minor) ? -1 : 1;
        } else if (this.patch != version.patch) {
            return (this.patch < version.patch) ? -1 : 1;
        }

        return 0;
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

    public boolean isEnterprise() {
        for (String enterprise : ENTERPRISES) {
            // Maybe should ignore case?
            if (origin.contains(enterprise)) {
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

        return major == that.major && minor == that.minor && patch == that.patch;
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
        if (origin.isEmpty()) {
            return String.format("%d.%d.%d", major, minor, patch);
        } else {
            return origin;
        }
    }

    /**
     * Parse a {@link ServerVersion} from {@link String}.
     *
     * @param version origin version string.
     * @return A {@link ServerVersion} that value decode from {@code version}.
     * @throws IllegalArgumentException if {@code version} is null, or any version part overflows to a negative integer.
     */
    public static ServerVersion parse(String version) {
        requireNonNull(version, "version must not be null");

        int length = version.length();
        int[] index = new int[]{0};
        int major = readInt(version, length, index);

        if (index[0] >= length) {
            // End-of-string.
            return create0(version, major, 0, 0);
        } else if (version.charAt(index[0]) != '.') {
            // Is not '.', has only postfix after major.
            return create0(version, major, 0, 0);
        } else {
            // Skip last '.' after major.
            ++index[0];
        }

        int minor = readInt(version, length, index);

        if (index[0] >= length) {
            return create0(version, major, minor, 0);
        } else if (version.charAt(index[0]) != '.') {
            // Is not '.', has only postfix after minor.
            return create0(version, major, minor, 0);
        } else {
            // Skip last '.' after minor.
            ++index[0];
        }

        return create0(version, major, minor, readInt(version, length, index));
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
        return create0("", major, minor, patch);
    }

    private static ServerVersion create0(String origin, int major, int minor, int patch) {
        require(major >= 0, "major version must not be a negative integer");
        require(minor >= 0, "minor version must not be a negative integer");
        require(patch >= 0, "patch version must not be a negative integer");

        return new ServerVersion(origin, major, minor, patch);
    }

    /**
     * C-style number parse, it most like {@code ByteBuf.read*(...)} and use external read index.
     *
     * @param input  the input string
     * @param length the size of {@code version}
     * @param index  reference of external read index, it will increase to the location of read
     * @return the integer read by {@code input}
     */
    private static int readInt(String input, int length, int[/* 1 */] index) {
        int ans = 0, i;
        char ch;

        for (i = index[0]; i < length; ++i) {
            ch = input.charAt(i);

            if (ch < '0' || ch > '9') {
                break;
            } else {
                ans = ans * 10 + (ch - '0');
            }
        }

        index[0] = i;

        return ans;
    }
}
