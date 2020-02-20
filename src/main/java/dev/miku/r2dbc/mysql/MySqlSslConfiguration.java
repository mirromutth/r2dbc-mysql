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

package dev.miku.r2dbc.mysql;

import dev.miku.r2dbc.mysql.constant.SslMode;
import io.netty.handler.ssl.SslContextBuilder;
import reactor.util.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;

import static dev.miku.r2dbc.mysql.util.InternalArrays.EMPTY_STRINGS;
import static dev.miku.r2dbc.mysql.util.AssertUtils.require;
import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * MySQL configuration of SSL.
 */
public final class MySqlSslConfiguration {

    private static final MySqlSslConfiguration DISABLED = new MySqlSslConfiguration(SslMode.DISABLED, EMPTY_STRINGS, null, null, null, null, null);

    private final SslMode sslMode;

    private final String[] tlsVersion;

    @Nullable
    private final String sslCa;

    @Nullable
    private final String sslKey;

    @Nullable
    private final CharSequence sslKeyPassword;

    @Nullable
    private final String sslCert;

    @Nullable
    private final Function<SslContextBuilder, SslContextBuilder> sslContextBuilderCustomizer;

    private MySqlSslConfiguration(
        SslMode sslMode, String[] tlsVersion, @Nullable String sslCa,
        @Nullable String sslKey, @Nullable CharSequence sslKeyPassword, @Nullable String sslCert,
        @Nullable Function<SslContextBuilder, SslContextBuilder> sslContextBuilderCustomizer
    ) {
        this.sslMode = sslMode;
        this.tlsVersion = tlsVersion;
        this.sslCa = sslCa;
        this.sslKey = sslKey;
        this.sslKeyPassword = sslKeyPassword;
        this.sslCert = sslCert;
        this.sslContextBuilderCustomizer = sslContextBuilderCustomizer;
    }

    public SslMode getSslMode() {
        return sslMode;
    }

    public String[] getTlsVersion() {
        return tlsVersion;
    }

    @Nullable
    public String getSslCa() {
        return sslCa;
    }

    @Nullable
    public String getSslKey() {
        return sslKey;
    }

    @Nullable
    public CharSequence getSslKeyPassword() {
        return sslKeyPassword;
    }

    @Nullable
    public String getSslCert() {
        return sslCert;
    }

    public SslContextBuilder customizeSslContext(SslContextBuilder builder) {
        if (sslContextBuilderCustomizer == null) {
            return builder;
        }

        return sslContextBuilderCustomizer.apply(builder);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MySqlSslConfiguration)) {
            return false;
        }
        MySqlSslConfiguration that = (MySqlSslConfiguration) o;
        return sslMode == that.sslMode &&
            Arrays.equals(tlsVersion, that.tlsVersion) &&
            Objects.equals(sslCa, that.sslCa) &&
            Objects.equals(sslKey, that.sslKey) &&
            Objects.equals(sslKeyPassword, that.sslKeyPassword) &&
            Objects.equals(sslCert, that.sslCert) &&
            Objects.equals(sslContextBuilderCustomizer, that.sslContextBuilderCustomizer);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(sslMode, sslCa, sslKey, sslKeyPassword, sslCert, sslContextBuilderCustomizer);
        result = 31 * result + Arrays.hashCode(tlsVersion);
        return result;
    }

    @Override
    public String toString() {
        if (sslMode.startSsl()) {
            return String.format("MySqlSslConfiguration{sslMode=%s, tlsVersion=%s, sslCa='%s', sslKey='%s', sslKeyPassword=REDACTED, sslCert='%s', sslContextBuilderCustomizer=%s}",
                sslMode, Arrays.toString(tlsVersion), sslCa, sslKey, sslCert, sslContextBuilderCustomizer);
        }

        return "DISABLED";
    }

    static MySqlSslConfiguration disabled() {
        return DISABLED;
    }

    static MySqlSslConfiguration create(
        SslMode sslMode, String[] tlsVersion, @Nullable String sslCa,
        @Nullable String sslKey, @Nullable CharSequence sslKeyPassword, @Nullable String sslCert,
        @Nullable Function<SslContextBuilder, SslContextBuilder> sslContextBuilderCustomizer
    ) {
        requireNonNull(sslMode, "sslMode must not be null");

        if (!sslMode.startSsl()) {
            return DISABLED;
        }

        requireNonNull(tlsVersion, "tlsVersion must not be null");
        require(!sslMode.verifyCertificate() || sslCa != null, "sslCa must not be null when verifying mode has set");
        require((sslKey == null && sslCert == null) || (sslKey != null && sslCert != null), "sslKey and cert must be both null or both non-null");

        return new MySqlSslConfiguration(sslMode, tlsVersion, sslCa, sslKey, sslKeyPassword, sslCert, sslContextBuilderCustomizer);
    }
}
