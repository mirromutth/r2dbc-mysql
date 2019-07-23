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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A helper for loading a factory that config is come from docker compose file.
 */
final class MySQLHelper {

    private static final Logger logger = LoggerFactory.getLogger(MySQLHelper.class);

    private static final ConcurrentMap<String, MySqlConnectionFactory> CONNECTION_FACTORY_MAP = new ConcurrentHashMap<>();

    static MySqlConnectionFactory getFactoryByVersion(String version, boolean ssl) {
        MySqlConnectionFactory nowFactory = CONNECTION_FACTORY_MAP.get(version);

        if (nowFactory == null) {
            logger.info("Version {} connection factory not found, try build a new factory", version);

            MySqlConnectionConfiguration newConfig;

            try {
                newConfig = buildConfig(version, ssl);
            } catch (IOException e) {
                throw new IllegalStateException("Read configuration failed", e);
            }

            MySqlConnectionFactory newFactory = MySqlConnectionFactory.from(newConfig);
            MySqlConnectionFactory lastFactory = CONNECTION_FACTORY_MAP.putIfAbsent(version, newFactory);

            if (lastFactory == null) {
                logger.info("Version {} connection factory build success, try init", version);
                initMySQL(newConfig);
                return newFactory;
            } else {
                logger.info("Version {} connection factory already build by other thread", version);
                return lastFactory;
            }
        } else {
            logger.debug("Version {} connection factory found, use factory on cache", version);
            return nowFactory;
        }
    }

    private static void initMySQL(MySqlConnectionConfiguration configuration) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("JDBC driver not found", e);
        }

        // Disable connector/J SSL for testing more fast.
        String url = String.format("jdbc:mysql://%s:%d?useSSL=false", configuration.getHost(), configuration.getPort());

        try (Connection conn = DriverManager.getConnection(url, configuration.getUsername(), configuration.getPassword().toString())) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE DATABASE IF NOT EXISTS `r2dbc`");
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Init MySQL database failed!", e);
        }
    }

    private static MySqlConnectionConfiguration buildConfig(String version, boolean ssl) throws IOException {
        String filename = buildFilename(version);
        InputStream resource = MySQLHelper.class.getClassLoader().getResourceAsStream(filename);

        if (resource == null) {
            throw new FileNotFoundException("File '" + filename + "' not found in resources.");
        }

        try (InputStream input = resource) {
            Map<String, Object> obj = new Yaml().load(input);
            Map<String, Object> service = getMap(getMap(obj, "services"), buildServiceName(version));
            CharSequence password = getRootPassword(getMap(service, "environment"));
            int port = Integer.parseInt(getPorts(service).get(0).split(":")[0]);

            MySqlConnectionConfiguration.Builder builder = MySqlConnectionConfiguration.builder()
                .host("127.0.0.1")
                .port(port)
                .connectTimeout(Duration.ofSeconds(5));

            if (ssl) {
                builder.enableSsl(MySqlSslConfiguration.Builder::disableServerVerify); // TODO: should verify server cert
            }

            return builder.username("root")
                .password(password)
                .database("r2dbc")
                .build();
        }
    }

    private static List<String> getPorts(Map<String, Object> map) {
        @SuppressWarnings("unchecked")
        List<String> l = (List<String>) map.get("ports");

        if (l == null) {
            throw new IllegalStateException("Key '" + "' not found in " + map);
        }

        return l;
    }

    private static CharSequence getRootPassword(Map<String, Object> map) {
        CharSequence m = (CharSequence) map.get("MYSQL_ROOT_PASSWORD");

        if (m == null) {
            throw new IllegalStateException("Key '" + "' not found in " + map);
        }

        return m;
    }

    private static Map<String, Object> getMap(Map<String, Object> map, String key) {
        @SuppressWarnings("unchecked")
        Map<String, Object> m = (Map<String, Object>) map.get(key);

        if (m == null) {
            throw new IllegalStateException("Key '" + "' not found in " + map);
        }

        return m;
    }

    private static String buildServiceName(String version) {
        return String.format("test-mysql%s-r2dbc", version);
    }

    private static String buildFilename(String version) {
        return String.format("mysql%s.dc.yml", version);
    }
}
