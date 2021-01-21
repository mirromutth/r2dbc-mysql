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

import dev.miku.r2dbc.mysql.constant.SslMode;
import io.netty.handler.ssl.SslContextBuilder;
import reactor.util.annotation.Nullable;

import javax.net.ssl.HostnameVerifier;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;
import static dev.miku.r2dbc.mysql.util.InternalArrays.EMPTY_STRINGS;

/**
 * MySQL configuration of SSL.
 */
public final class MySqlSslConfiguration {

    private static final MySqlSslConfiguration DISABLED = new MySqlSslConfiguration(SslMode.DISABLED,
        EMPTY_STRINGS, null, null, null, null, null, null);

    private final SslMode sslMode;

    private final String[] tlsVersion;

    @Nullable
    private final HostnameVerifier sslHostnameVerifier;

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

    private MySqlSslConfiguration(SslMode sslMode, String[] tlsVersion,
        @Nullable HostnameVerifier sslHostnameVerifier, @Nullable String sslCa, @Nullable String sslKey,
        @Nullable CharSequence sslKeyPassword, @Nullable String sslCert,
        @Nullable Function<SslContextBuilder, SslContextBuilder> sslContextBuilderCustomizer) {
        this.sslMode = sslMode;
        this.tlsVersion = tlsVersion;
        this.sslHostnameVerifier = sslHostnameVerifier;
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
    public HostnameVerifier getSslHostnameVerifier() {
        return sslHostnameVerifier;
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

    /**
     * Customizes a {@link SslContextBuilder} that customizer was specified by configuration, or do nothing if
     * the customizer was not set.
     *
     * @param builder the {@link SslContextBuilder}.
     * @return the {@code builder}.
     */
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
            Objects.equals(sslHostnameVerifier, that.sslHostnameVerifier) &&
            Objects.equals(sslCa, that.sslCa) &&
            Objects.equals(sslKey, that.sslKey) &&
            Objects.equals(sslKeyPassword, that.sslKeyPassword) &&
            Objects.equals(sslCert, that.sslCert) &&
            Objects.equals(sslContextBuilderCustomizer, that.sslContextBuilderCustomizer);
    }

    @Override
    public int hashCode() {
        int hash = Objects.hash(sslMode, sslHostnameVerifier, sslCa, sslKey, sslKeyPassword, sslCert,
            sslContextBuilderCustomizer);
        return 31 * hash + Arrays.hashCode(tlsVersion);
    }

    @Override
    public String toString() {
        if (sslMode == SslMode.DISABLED) {
            return "MySqlSslConfiguration{sslMode=DISABLED}";
        }

        return "MySqlSslConfiguration{sslMode=" + sslMode + ", tlsVersion=" + Arrays.toString(tlsVersion) +
            ", sslHostnameVerifier=" + sslHostnameVerifier + ", sslCa='" + sslCa + "', sslKey='" + sslKey +
            "', sslKeyPassword=REDACTED, sslCert='" + sslCert + "', sslContextBuilderCustomizer=" +
            sslContextBuilderCustomizer + '}';
    }

    static MySqlSslConfiguration disabled() {
        return DISABLED;
    }

    static MySqlSslConfiguration create(SslMode sslMode, String[] tlsVersion,
        @Nullable HostnameVerifier sslHostnameVerifier, @Nullable String sslCa, @Nullable String sslKey,
        @Nullable CharSequence sslKeyPassword, @Nullable String sslCert,
        @Nullable Function<SslContextBuilder, SslContextBuilder> sslContextBuilderCustomizer) {
        requireNonNull(sslMode, "sslMode must not be null");

        if (sslMode == SslMode.DISABLED) {
            return DISABLED;
        }

        requireNonNull(tlsVersion, "tlsVersion must not be null");

        return new MySqlSslConfiguration(sslMode, tlsVersion, sslHostnameVerifier, sslCa, sslKey,
            sslKeyPassword, sslCert, sslContextBuilderCustomizer);
    }
}
