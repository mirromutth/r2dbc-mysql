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
import dev.miku.r2dbc.mysql.constant.ZeroDateOption;
import dev.miku.r2dbc.mysql.extension.Extension;
import io.netty.handler.ssl.SslContextBuilder;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.function.Predicate;

import static dev.miku.r2dbc.mysql.util.AssertUtils.require;
import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;
import static dev.miku.r2dbc.mysql.util.InternalArrays.EMPTY_STRINGS;

/**
 * MySQL configuration of connection.
 */
public final class MySqlConnectionConfiguration {

    /**
     * Default MySQL port.
     */
    private static final int DEFAULT_PORT = 3306;

    private static final Predicate<String> DEFAULT_SERVER_PREPARE = sql -> false;

    /**
     * {@code true} if {@link #domain} is hostname, otherwise {@link #domain} is unix domain socket path.
     */
    private final boolean isHost;

    /**
     * Domain of connecting, may be hostname or unix domain socket path.
     */
    private final String domain;

    private final int port;

    private final MySqlSslConfiguration ssl;

    @Nullable
    private final Duration connectTimeout;

    private final ZeroDateOption zeroDateOption;

    private final String user;

    @Nullable
    private final CharSequence password;

    private final String database;

    @Nullable
    private final Predicate<String> preferPrepareStatement;

    private final Extensions extensions;

    private MySqlConnectionConfiguration(
        boolean isHost, String domain, int port, @Nullable MySqlSslConfiguration ssl,
        @Nullable Duration connectTimeout, ZeroDateOption zeroDateOption,
        String user, @Nullable CharSequence password, @Nullable String database,
        @Nullable Predicate<String> preferPrepareStatement, Extensions extensions
    ) {
        this.isHost = isHost;
        this.domain = domain;
        this.port = port;
        this.connectTimeout = connectTimeout;
        this.ssl = requireNonNull(ssl, "ssl must not be null");
        this.zeroDateOption = requireNonNull(zeroDateOption, "zeroDateOption must not be null");
        this.user = requireNonNull(user, "user must not be null");
        this.password = password;
        this.database = database == null || database.isEmpty() ? "" : database;
        this.preferPrepareStatement = preferPrepareStatement;
        this.extensions = requireNonNull(extensions, "extensions must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    boolean isHost() {
        return isHost;
    }

    String getDomain() {
        return domain;
    }

    int getPort() {
        return port;
    }

    @Nullable
    Duration getConnectTimeout() {
        return connectTimeout;
    }

    MySqlSslConfiguration getSsl() {
        return ssl;
    }

    ZeroDateOption getZeroDateOption() {
        return zeroDateOption;
    }

    String getUser() {
        return user;
    }

    @Nullable
    CharSequence getPassword() {
        return password;
    }

    String getDatabase() {
        return database;
    }

    @Nullable
    Predicate<String> getPreferPrepareStatement() {
        return preferPrepareStatement;
    }

    Extensions getExtensions() {
        return extensions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MySqlConnectionConfiguration)) {
            return false;
        }
        MySqlConnectionConfiguration that = (MySqlConnectionConfiguration) o;
        return isHost == that.isHost &&
            domain.equals(that.domain) &&
            port == that.port &&
            ssl.equals(that.ssl) &&
            Objects.equals(connectTimeout, that.connectTimeout) &&
            zeroDateOption == that.zeroDateOption &&
            user.equals(that.user) &&
            Objects.equals(password, that.password) &&
            database.equals(that.database) &&
            Objects.equals(preferPrepareStatement, that.preferPrepareStatement) &&
            extensions.equals(that.extensions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isHost, domain, port, ssl, connectTimeout, zeroDateOption, user, password, database, preferPrepareStatement, extensions);
    }

    @Override
    public String toString() {
        if (isHost) {
            return String.format("MySqlConnectionConfiguration{host=%s, port=%d, ssl=%s, connectTimeout=%s, zeroDateOption=%s, user='%s', password=REDACTED, database='%s', " +
                    "preferPrepareStatement=%s, extensions=%s}",
                domain, port, ssl, connectTimeout, zeroDateOption, user, database, preferPrepareStatement, extensions);
        } else {
            return String.format("MySqlConnectionConfiguration{unixSocket=%s, connectTimeout=%s, zeroDateOption=%s, user='%s', password=REDACTED, database='%s', preferPrepareStatement=%s, " +
                    "extensions=%s}",
                domain, connectTimeout, zeroDateOption, user, database, preferPrepareStatement, extensions);
        }
    }

    public static final class Builder {

        @Nullable
        private String database;

        private boolean isHost = true;

        private String domain;

        @Nullable
        private CharSequence password;

        private int port = DEFAULT_PORT;

        @Nullable
        private Duration connectTimeout;

        private String user;

        private ZeroDateOption zeroDateOption = ZeroDateOption.USE_NULL;

        @Nullable
        private SslMode sslMode;

        private String[] tlsVersion = EMPTY_STRINGS;

        @Nullable
        private String sslCa;

        @Nullable
        private String sslKey;

        @Nullable
        private CharSequence sslKeyPassword;

        @Nullable
        private String sslCert;

        @Nullable
        private Function<SslContextBuilder, SslContextBuilder> sslContextBuilderCustomizer;

        @Nullable
        private Predicate<String> preferPrepareStatement;

        private boolean autodetectExtensions = true;

        private final List<Extension> extensions = new ArrayList<>();

        private Builder() {
        }

        public MySqlConnectionConfiguration build() {
            SslMode sslMode = requireSslMode();

            if (isHost) {
                requireNonNull(domain, "host must not be null when using TCP socket");
                require(port >= 0 && port <= 0xFFFF, "port must be between 0 and 65535 when using TCP socket");
            } else {
                requireNonNull(domain, "unixSocket must not be null when using unix domain socket");
                require(!sslMode.startSsl(), "sslMode must be disabled when using unix domain socket");
            }

            MySqlSslConfiguration ssl = MySqlSslConfiguration.create(sslMode, tlsVersion, sslCa, sslKey, sslKeyPassword, sslCert, sslContextBuilderCustomizer);
            return new MySqlConnectionConfiguration(isHost, domain, port, ssl, connectTimeout, zeroDateOption, user, password, database, preferPrepareStatement,
                Extensions.from(extensions, autodetectExtensions));
        }

        /**
         * Configure the database.  Default no database.
         *
         * @param database the database, or {@code null} if no database want to be login.
         * @return this {@link Builder}
         */
        public Builder database(@Nullable String database) {
            this.database = database;
            return this;
        }

        /**
         * Configure the Unix Domain Socket to connect to.
         *
         * @param unixSocket the socket file path.
         * @return this {@link Builder}.
         * @throws IllegalArgumentException if {@code unixSocket} is {@code null}
         */
        public Builder unixSocket(String unixSocket) {
            this.domain = requireNonNull(unixSocket, "unixSocket must not be null");
            this.isHost = false;
            return this;
        }

        /**
         * Configure the host.
         *
         * @param host the host
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code host} is {@code null}
         */
        public Builder host(String host) {
            this.domain = requireNonNull(host, "host must not be null");
            this.isHost = true;
            return this;
        }

        /**
         * Configure the password, MySQL allows to login without password.
         * <p>
         * Note: for memory security, should not use intern {@link String} for password.
         *
         * @param password the password, or {@code null} when user has no password.
         * @return this {@link Builder}
         */
        public Builder password(@Nullable CharSequence password) {
            this.password = password;
            return this;
        }

        /**
         * Configure the port.  Defaults to {@code 3306}.
         *
         * @param port the port
         * @return this {@link Builder}
         * @throws IllegalArgumentException if the {@code port} is negative or bigger than {@literal 65535}.
         */
        public Builder port(int port) {
            require(port >= 0 && port <= 0xFFFF, "port must be between 0 and 65535");

            this.port = port;
            return this;
        }

        /**
         * Configure the connection timeout.  Default no timeout.
         *
         * @param connectTimeout the connection timeout, or {@code null} if has no timeout.
         * @return this {@link Builder}
         */
        public Builder connectTimeout(@Nullable Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        /**
         * Set the user for login the database.
         *
         * @param user the user.
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code user} is {@code null}
         */
        public Builder user(String user) {
            this.user = requireNonNull(user, "user must not be null");
            return this;
        }

        /**
         * An alias of {@link #user(String)}.
         *
         * @param user the user.
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code user} is {@code null}
         */
        public Builder username(String user) {
            return user(user);
        }

        /**
         * Configure the behavior when this driver receives a value of zero-date.
         * See also {@link ZeroDateOption}.
         *
         * @param zeroDate the {@link ZeroDateOption}.
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code zeroDate} is {@code null}
         */
        public Builder zeroDateOption(ZeroDateOption zeroDate) {
            this.zeroDateOption = requireNonNull(zeroDate, "zeroDateOption must not be null");
            return this;
        }

        /**
         * Configure ssl mode.  See also {@link SslMode}.
         *
         * @param sslMode the SSL mode to use.
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code sslMode} is {@code null}
         */
        public Builder sslMode(SslMode sslMode) {
            this.sslMode = requireNonNull(sslMode, "sslMode must not be null");
            return this;
        }

        /**
         * Configure TLS versions, see {@link dev.miku.r2dbc.mysql.constant.TlsVersions}.
         *
         * @param tlsVersion TLS versions
         * @return this {@link Builder}
         * @throws IllegalArgumentException if the array {@code tlsVersion} is {@code null}.
         */
        public Builder tlsVersion(String... tlsVersion) {
            requireNonNull(tlsVersion, "tlsVersion must not be null");

            int size = tlsVersion.length;

            if (size > 0) {
                String[] versions = new String[size];
                System.arraycopy(tlsVersion, 0, versions, 0, size);
                this.tlsVersion = versions;
            } else {
                this.tlsVersion = EMPTY_STRINGS;
            }
            return this;
        }

        /**
         * Configure ssl root cert for server certificate validation.
         *
         * @param sslCa an X.509 certificate chain file in PEM format
         * @return this {@link Builder}
         */
        public Builder sslCa(@Nullable String sslCa) {
            this.sslCa = sslCa;
            return this;
        }

        /**
         * Configure ssl key and ssl cert for client certificate authentication.
         * <p>
         * The {@code sslCert} and {@code sslKey} must be both non-{@code null}
         * or both {@code null}.
         *
         * @param sslCert an X.509 certificate chain file in PEM format
         * @param sslKey  a PKCS#8 private key file in PEM format, should be not password-protected
         * @return this {@link Builder}
         * @throws IllegalArgumentException if one of {@code sslCert} and {@code sslKey} is {@code null},
         * and the other is non-{@code null}.
         */
        public Builder sslKeyAndCert(@Nullable String sslCert, @Nullable String sslKey) {
            return sslKeyAndCert(sslCert, sslKey, null);
        }

        /**
         * Configure ssl key and ssl cert for client certificate authentication.
         * <p>
         * The {@code sslCert} and {@code sslKey} must be both non-{@code null}
         * or both {@code null}.
         *
         * @param sslCert        an X.509 certificate chain file in PEM format
         * @param sslKey         a PKCS#8 private key file in PEM format
         * @param sslKeyPassword the password of the {@code sslKey}, or {@code null} if it's not password-protected
         * @return this {@link Builder}
         * @throws IllegalArgumentException if one of {@code sslCert} and {@code sslKey} is {@code null},
         * and the other is non-{@code null}.
         */
        public Builder sslKeyAndCert(@Nullable String sslCert, @Nullable String sslKey, @Nullable CharSequence sslKeyPassword) {
            require((sslCert == null && sslKey == null) || (sslCert != null && sslKey != null), "SSL key and cert must be both null or both non-null");

            this.sslCert = sslCert;
            this.sslKey = sslKey;
            this.sslKeyPassword = sslKeyPassword;
            return this;
        }

        /**
         * Configure a {@link SslContextBuilder} customizer. The customizer gets applied on each SSL
         * connection attempt to allow for just-in-time configuration updates. The {@link Function}
         * gets called with the prepared {@link SslContextBuilder} that has all configuration options
         * applied. The customizer may return the same builder or return a new builder instance to
         * be used to build the SSL context.
         *
         * @param customizer customizer function
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code customizer} is {@code null}
         */
        public Builder sslContextBuilderCustomizer(Function<SslContextBuilder, SslContextBuilder> customizer) {
            requireNonNull(customizer, "sslContextBuilderCustomizer must not be null");

            this.sslContextBuilderCustomizer = customizer;
            return this;
        }

        /**
         * Configure the protocol of parametrized statements to the text protocol.
         * <p>
         * The text protocol is default protocol that's using client-preparing.
         * See also MySQL documentations.
         *
         * @return this {@link Builder}
         */
        public Builder useClientPrepareStatement() {
            this.preferPrepareStatement = null;
            return this;
        }

        /**
         * Configure the protocol of parametrized statements to the binary protocol.
         * <p>
         * The binary protocol is compact protocol that's using server-preparing.
         * See also MySQL documentations.
         *
         * @return this {@link Builder}
         */
        public Builder useServerPrepareStatement() {
            return useServerPrepareStatement(DEFAULT_SERVER_PREPARE);
        }

        /**
         * Configure the protocol of parametrized statements and prepare-preferred
         * simple statements to the binary protocol.
         * <p>
         * The {@code preferPrepareStatement} configures whether to prefer prepare execution
         * on a statement-by-statement basis (simple statements). The {@link Predicate} accepts
         * the simple SQL query string and returns a boolean flag indicating preference.
         * {@code true} prepare-preferred, {@code false} prefers direct execution (text protocol).
         * Defaults to direct execution.
         * <p>
         * The binary protocol is compact protocol that's using server-preparing.
         * See also MySQL documentations.
         *
         * @param preferPrepareStatement the above {@link Predicate}.
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code preferPrepareStatement} is {@code null}
         */
        public Builder useServerPrepareStatement(Predicate<String> preferPrepareStatement) {
            requireNonNull(preferPrepareStatement, "preferPrepareStatement must not be null");

            this.preferPrepareStatement = preferPrepareStatement;
            return this;
        }

        /**
         * Configures whether to use {@link ServiceLoader} to discover and register extensions.
         * Defaults to {@code true}.
         *
         * @param autodetectExtensions to discover and register extensions
         * @return this {@link Builder}
         */
        public Builder autodetectExtensions(boolean autodetectExtensions) {
            this.autodetectExtensions = autodetectExtensions;
            return this;
        }

        /**
         * Registers a {@link Extension} to extend driver functionality and manually.
         * <p>
         * Notice: the driver will not deduplicate {@link Extension}s of autodetect
         * discovered and manually extended. So if a {@link Extension} is registered by
         * this function and autodetect discovered, it will get two {@link Extension}
         * as same.
         *
         * @param extension extension to extend driver functionality
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code extension} is {@code null}.
         */
        public Builder extendWith(Extension extension) {
            this.extensions.add(requireNonNull(extension, "extension must not be null"));
            return this;
        }

        private SslMode requireSslMode() {
            SslMode sslMode = this.sslMode;

            if (sslMode == null) {
                sslMode = isHost ? SslMode.PREFERRED : SslMode.DISABLED;
            }

            return sslMode;
        }
    }
}
