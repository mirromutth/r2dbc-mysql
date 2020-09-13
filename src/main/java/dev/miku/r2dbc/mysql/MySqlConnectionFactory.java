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

import dev.miku.r2dbc.mysql.cache.Caches;
import dev.miku.r2dbc.mysql.cache.PrepareCache;
import dev.miku.r2dbc.mysql.cache.QueryCache;
import dev.miku.r2dbc.mysql.client.Client;
import dev.miku.r2dbc.mysql.codec.Codecs;
import dev.miku.r2dbc.mysql.codec.CodecsBuilder;
import dev.miku.r2dbc.mysql.constant.SslMode;
import dev.miku.r2dbc.mysql.extension.CodecRegistrar;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.unix.DomainSocketAddress;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.function.Predicate;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link ConnectionFactory} for creating connections to a MySQL database.
 */
public final class MySqlConnectionFactory implements ConnectionFactory {

    private final Mono<MySqlConnection> client;

    private MySqlConnectionFactory(Mono<MySqlConnection> client) {
        this.client = client;
    }

    @Override
    public Mono<MySqlConnection> create() {
        return client;
    }

    @Override
    public ConnectionFactoryMetadata getMetadata() {
        return MySqlConnectionFactoryMetadata.INSTANCE;
    }

    public static MySqlConnectionFactory from(MySqlConnectionConfiguration configuration) {
        requireNonNull(configuration, "configuration must not be null");

        LazyQueryCache queryCache = new LazyQueryCache(configuration.getQueryCacheSize());

        return new MySqlConnectionFactory(Mono.defer(() -> {
            MySqlSslConfiguration ssl;
            SocketAddress address;

            if (configuration.isHost()) {
                ssl = configuration.getSsl();
                address = InetSocketAddress.createUnresolved(configuration.getDomain(), configuration.getPort());
            } else {
                ssl = MySqlSslConfiguration.disabled();
                address = new DomainSocketAddress(configuration.getDomain());
            }

            String database = configuration.getDatabase();
            String user = configuration.getUser();
            CharSequence password = configuration.getPassword();
            SslMode sslMode = ssl.getSslMode();
            ConnectionContext context = new ConnectionContext(configuration.getZeroDateOption(), configuration.getServerZoneId());
            Extensions extensions = configuration.getExtensions();
            Predicate<String> prepare = configuration.getPreferPrepareStatement();
            int prepareCacheSize = configuration.getPrepareCacheSize();

            return Client.connect(ssl, address, configuration.isTcpKeepAlive(), configuration.isTcpNoDelay(), context, configuration.getConnectTimeout())
                .flatMap(client -> QueryFlow.login(client, sslMode, database, user, password, context))
                .flatMap(client -> {
                    ByteBufAllocator allocator = client.getByteBufAllocator();
                    CodecsBuilder builder = Codecs.builder(allocator);
                    PrepareCache<Integer> prepareCache = Caches.createPrepareCache(prepareCacheSize);

                    extensions.forEach(CodecRegistrar.class, registrar -> registrar.register(allocator, builder));

                    return MySqlConnection.init(client, builder.build(), context, queryCache.get(), prepareCache, prepare);
                });
        }));
    }

    private static final class LazyQueryCache {

        private final int capacity;

        @Nullable
        private volatile QueryCache cache;

        private LazyQueryCache(int capacity) {
            this.capacity = capacity;
        }

        public QueryCache get() {
            QueryCache cache = this.cache;
            if (cache == null) {
                synchronized (this) {
                    if ((cache = this.cache) == null) {
                        this.cache = cache = Caches.createQueryCache(capacity);
                    }
                    return cache;
                }
            }
            return cache;
        }
    }
}
