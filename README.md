# Reactive Relational Database Connectivity MySQL Implementation [![r2dbc-mysql](https://jitpack.io/v/mirromutth/r2dbc-mysql.svg)](https://jitpack.io/#mirromutth/r2dbc-mysql)

This project contains the [MySQL][m] implementation of the [R2DBC SPI](https://github.com/r2dbc/r2dbc-spi).
This implementation is not intended to be used directly, but rather to be
used as the backing implementation for a humane client library to
delegate to. See [R2DBC Homepage](https://r2dbc.io).

This driver provides the following features:

- [x] Support MySQL database server versions between 5.5 to 8.x.
- [x] Login with username/password (or no password)
- [x] Execution of simple or batch statements without bindings (MySQL use text result for simple or batch statements).
- [x] Execution of prepared statements with bindings (MySQL use binary result for prepared statements).
- [x] Reactive LOB types (e.g. BLOB, CLOB)
- [x] All charsets from MySQL, like `utf8mb4_0900_ai_ci`, `latin1_general_ci`, `utf32_unicode_520_ci`, etc.
- [x] All authentication types for MySQL, like `caching_sha2_password`, `mysql_native_password`, etc.
- [x] General exceptions of error code and standard SQL state mappings.
- [x] Secure connection with verification (SSL/TLS), auto-select TLS version for community and enterprise editions.
- [x] Transactions with savepoint.
- [x] Native ping command. (**unstable**, target is a future SPI that can be verifying with the selectable depth)
- [ ] Prepared statements cache.
- [ ] Statement parser cache.

## Maven

```xml
<dependency>
    <groupId>com.github.mirromutth</groupId>
    <artifactId>r2dbc-mysql</artifactId>
    <version>0.2.0.M2</version>
</dependency>
```

> Note: `com.github.mirromutth` is temporary group ID, it may change during its release or next milestone.

Artifacts can be found at the following repositories.

### Repositories

```xml
<repository>
    <id>jitpack.io</id>
    <url>https://jitpack.io</url>
</repository>
```

## Gradle

### Groovy DSL

```groovy
repositories {
    maven { url 'https://jitpack.io' }
    // ...
}
// ...
dependencies {
    implementation 'com.github.mirromutth:r2dbc-mysql:0.2.0.M2'
}
```

### Kotlin DSL

```kotlin
repositories {
    maven("https://jitpack.io")
    // ...
}
// ...
dependencies {
    compile("com.github.mirromutth:r2dbc-mysql:0.2.0.M2")
}
```

## Getting Started

Here is a quick teaser of how to use R2DBC MySQL in Java:

### URL Connection Factory Discovery

```java
// Notice: the query string must be URL encoded
ConnectionFactory connectionFactory = ConnectionFactories.get(
    "r2dbcs:mysql://root:database-password-in-here@127.0.0.1:3306/r2dbc?" +
    "zeroDate=use_round&" +
    "sslMode=verify_identity&" +
    "tlsVersion=TLSv1.1%2CTLSv1.2%2CTLSv1.3&" +
    "sslCa=%2Fpath%2Fto%2Fmysql%2Fca.pem&" +
    "sslKey=%2Fpath%2Fto%2Fmysql%2Fclient-key.pem&" +
    "sslCert=%2Fpath%2Fto%2Fmysql%2Fclient-cert.pem&" +
    "sslKeyPassword=key-pem-password-in-here"
)

// Creating a Mono using Project Reactor
Mono<Connection> connectionMono = Mono.from(connectionFactory.create());
```

### Programmatic Connection Factory Discovery

```java
ConnectionFactoryOptions options = ConnectionFactoryOptions.builder()
    .option(DRIVER, "mysql")
    .option(HOST, "127.0.0.1")
    .option(USER, "root")
    .option(PORT, 3306)  // optional, default 3306
    .option(PASSWORD, "database-password-in-here") // optional, default null, null means has no password
    .option(DATABASE, "r2dbc") // optional, default null, null means not specifying the database
    .option(CONNECT_TIMEOUT, Duration.ofSeconds(3)) // optional, default null, null means no timeout
    .option(SSL, true) // optional, default is enabled, it will be ignore if "sslMode" is set
    .option(Option.valueOf("sslMode"), "verify_identity") // optional, default "preferred"
    .option(Option.valueOf("sslCa"), "/path/to/mysql/ca.pem") // required when sslMode is verify_ca or verify_identity, default null, null means has no server CA cert
    .option(Option.valueOf("sslKey"), "/path/to/mysql/client-key.pem") // optional, default null, null means has no client key
    .option(Option.valueOf("sslCert"), "/path/to/mysql/client-cert.pem") // optional, default null, null means has no client cert
    .option(Option.valueOf("sslKeyPassword"), "key-pem-password-in-here") // optional, default null, null means has no password for client key (i.e. "sslKey")
    .option(Option.valueOf("tlsVersion"), "TLSv1.1,TLSv1.2,TLSv1.3") // optional, default is auto-selected by the server
    .option(Option.valueOf("zeroDate"), "use_null") // optional, default "use_null"
    .build();
ConnectionFactory connectionFactory = ConnectionFactories.get(options);

// Creating a Mono using Project Reactor
Mono<Connection> connectionMono = Mono.from(connectionFactory.create());
```

### Programmatic Configuration

```java
MySqlConnectConfiguration configuration = MySqlConnectConfiguration.builder()
    .host("127.0.0.1")
    .username("root")
    .port(3306) // optional, default 3306
    .password("database-password-in-here") // optional, default null, null means has no password
    .database("r2dbc") // optional, default null, null means not specifying the database
    .connectTimeout(Duration.ofSeconds(3)) // optional, default null, null means no timeout
    .sslMode(SslMode.VERIFY_IDENTITY) // optional, default SslMode.PREFERRED
    .sslCa("/path/to/mysql/ca.pem") // required when sslMode is VERIFY_CA or VERIFY_IDENTITY, default null, null means has no server CA cert
    .sslKeyAndCert("/path/to/mysql/client-cert.pem", "/path/to/mysql/client-key.pem", "key-pem-password-in-here") // optional, default has no client key and cert
    .tlsVersion(TlsVersions.TLS1_1, TlsVersions.TLS1_2, TlsVersions.TLS1_3) // optional, default is auto-selected by the server
    .zeroDateOption(ZeroDateOption.USE_NULL) // optional, default ZeroDateOption.USE_NULL
    .build();
ConnectionFactory connectionFactory = MySqlConnectionFactory.from(configuration);

// Creating a Mono using Project Reactor
Mono<Connection> connectionMono = Mono.from(connectionFactory.create());
```

### Configuration items

| name | valid values | required | description |
|---|---|---|---|
| driver | A constant "mysql" | Required in R2DBC discovery | This driver needs to be discovered by name in R2DBC |
| host | A hostname or IP | Required | The host of MySQL database server |
| port | A positive integer less than 65536 | Optional, default 3306 | The port of MySQL database server |
| username | A valid MySQL username and not be empty | Required | Who wants to connect to the MySQL database |
| password | Any printable string | Optional, default no password | The password of the MySQL database user |
| database | A valid MySQL database name | Optional, default does not initialize database | Database used by the MySQL connection |
| connectTimeout | A `Duration` which must be positive duration | Optional, default has no timeout | TCP connect timeout |
| sslMode | Any value of `SslMode` | Optional, default is `PREFERRED` | SSL mode, see following notice |
| sslCa | A path of local file which type is `PEM` | Required when `sslMode` is `VERIFY_CA` or `VERIFY_IDENTITY` | The CA cert of MySQL database server |
| sslCert | A path of local file which type is `PEM` | Required when `sslKey` exists | The SSL cert of client |
| sslKey | A path of local file which type is `PEM` | Required when `sslCert` exists | The SSL key of client |
| sslKeyPassword | Any valid password for `PEM` file | Optional, default `sslKey` has no password | The password for client SSL key (i.e. `sslKey`) |
| tlsVersion | Any value list of `TlsVersions` | Optional, default is auto-selected by the server | The TLS version for SSL, see following notice |
| zeroDateOption | Any value of `ZeroDateOption` | Optional, default `USE_NULL` | The option indicates "zero date" handling, see following notice |

- `SslMode`: Considers security level and verification for SSL, make sure the database server supports SSL before you want change SSL mode to `REQUIRED` or higher.
  - `DISABLED`: I don't care about security and don't want to pay the overhead for encryption
  - `PREFERRED` (default level): I don't care about encryption but will pay the overhead of encryption if the server supports it
  - `REQUIRED`: I want my data to be encrypted, and I accept the overhead. I trust that the network will make sure I always connect to the server I want
  - `VERIFY_CA`: I want my data encrypted, and I accept the overhead. I want to be sure that I connect to a server that I trust
  - `VERIFY_IDENTITY` (highest level, most like web browser): I want my data encrypted, and I accept the overhead. I want to be sure that I connect to a server I trust, and that it's the one I specify
- `TlsVersions`: Considers TLS version names for SSL, can be **multi-values** in configuration, make sure the database server supports selected TLS versions
  - `TLS1` (i.e. "TLSv1"): Under generic circumstances, MySQL database supports it if database supports SSL
  - `TLS1_1` (i.e. "TLSv1.1"): Under generic circumstances, MySQL database supports it if database supports SSL
  - `TLS1_2` (i.e. "TLSv1.2"): Supported only in Community Edition `8.0.4` or higher, otherwise in Enterprise Edition `5.6.0` or higher
  - `TLS1_3` (i.e. "TLSv1.3"): Supported only available as of MySQL `8.0.16` or higher, and requires compiling MySQL using OpenSSL `1.1.1` or higher
- `ZeroDateOption`: Considers special handling when MySQL database server returning "zero date" (i.e. `0000-00-00 00:00:00`)
  - `EXCEPTION`: Just throw a exception when MySQL database server return "zero date".
  - `USE_NULL`: Use `null` when MySQL database server return "zero date".
  - `USE_ROUND`: **NOT** RECOMMENDED, only for compatibility. Use "round" date (i.e. `0001-01-01 00:00:00`) when MySQL database server return "zero date".

Should use `enum` in [Programmatic](#programmatic-configuration) configuration that not like discovery configurations, except `TlsVersions` (All elements of `TlsVersions` will be always `String` which is case sensitive).

### Pooling

See [r2dbc-pool](https://github.com/r2dbc/r2dbc-pool).

### Simple statement

```java
connection.createStatement("INSERT INTO `person` (`first_name`, `last_name`) VALUES ('who', 'how')")
    .execute(); // return a Publisher include one Result
```

> Can only contains one statement (`SELECT`, `INSERT`, `UPDATE`, etc.).

### Prepared statement

```java
connection.createStatement("INSERT INTO `person` (`birth`, `nickname`, `show_name`) VALUES (?, ?name, ?name)")
    .bind(0, LocalDateTime.of(2019, 6, 25, 12, 12, 12))
    .bind("name", "Some one") // Not one-to-one binding, call twice of native index-bindings, or call once of name-bindings.
    .add()
    .bind(0, LocalDateTime.of(2009, 6, 25, 12, 12, 12))
    .bind(1, "My Nickname")
    .bind(2, "Naming show")
    .returnGeneratedValues("generated_id")
    .execute(); // return a Publisher include two Result.
```

> Can only contains one statement (`SELECT`, `INSERT`, `UPDATE`, etc.).

- All parameters must be bound before execute, even parameter is `null` (use `bindNull` to bind `null`).
- In one-to-one binding, because native MySQL prepared statements use index-based parameters, *index-bindings* will have **better** performance than *name-bindings*.

### Batch statement

```java
connection.createBatch()
    .add("INSERT INTO `person` (`first_name`, `last_name`) VALUES ('who', 'how')")
    .add("UPDATE `earth` SET `count` = `count` + 1 WHERE `id` = 'human'")
    .execute(); // return a Publisher include two Result
```

> The parameter of `add(String)` can only contains one statement, `;` will be removed if has only whitespace follow the `;`.

### Transactions

```java
connection.beginTransaction()
    .then(Mono.from(connection.createStatement("INSERT INTO `person` (`first_name`, `last_name`) VALUES ('who', 'how')").execute()))
    .flatMap(Result::getRowsUpdated)
    .thenMany(Flux.from(connection.createStatement("INSERT INTO `person` (`birth`, `nickname`, `show_name`) VALUES (?, ?name, ?name)")
        .bind(0, LocalDateTime.of(2019, 6, 25, 12, 12, 12))
        .bind("name", "Some one")
        .add()
        .bind(0, LocalDateTime.of(2009, 6, 25, 12, 12, 12))
        .bind(1, "My Nickname")
        .bind(2, "Naming show")
        .returnGeneratedValues("generated_id")
        .execute()))
    .concatMap(Result::getRowsUpdated) // Should consume one-by-one for multi-results
    .then(connection.commitTransaction());
```

## Data Type Mapping

This reference table shows the type mapping between [MySQL][m] and Java data types:

| MySQL Type | Unsigned | Support Data Type |
|---|---|---|
| INT | UNSIGNED | `Long`, `BigInteger` |
| INT | SIGNED | `Integer`, `Long`, `BigInteger` |
| TINYINT | UNSIGNED | `Short`, `Integer`, `Long`, `BigInteger` |
| TINYINT | SIGNED | `Byte`, `Short`, `Integer`, `Long`, `BigInteger` |
| SMALLINT | UNSIGNED | `Integer`, `Long`, `BigInteger` |
| SMALLINT | SIGNED | `Short`, `Integer`, `Long`, `BigInteger` |
| MEDIUMINT | SIGNED/UNSIGNED | `Integer`, `Long`, `BigInteger` |
| BIGINT | UNSIGNED | `BigInteger`, `Long` (Not check overflow for ID column) |
| BIGINT | SIGNED | `Long`, `BigInteger` |
| FLOAT | SIGNED/UNSIGNED | `Float`, `BigDecimal` |
| DOUBLE | SIGNED/UNSIGNED | `Double`, `BigDecimal`  |
| DECIMAL | SIGNED/UNSIGNED | `BigDecimal`, `Float` (Size less than 7), `Double` (Size less than 16) |
| BIT | - | `BitSet`, `Boolean` (Size is 1), `byte[]` |
| DATETIME/TIMESTAMP | - | `LocalDateTime` |
| DATE | - | `LocalDate` |
| TIME | - | `Duration`, `LocalTime` |
| YEAR | - | `Short`, `Integer`, `Long`, `BigInteger`, `Year` |
| VARCHAR/NVARCHAR | - | `String` |
| CHAR/NCHAR | - | `String` |
| ENUM | - | `String`, `Enum<?>` |
| SET | - | `String[]`, `String`, `Set<String>` and `Set<Enum<?>>` (`Set<T>` need use `ParameterizedType`) |
| BLOB (LONGBLOB, etc.) | - | `Blob`, `byte[]` (Not check overflow) |
| TEXT (LONGTEXT, etc.) | - | `Clob`, `String` (Not check overflow) |
| JSON | - | `String`, `Clob` |
| GEOMETRY | - | `byte[]`, `Blob` |

## Reporting Issues

The R2DBC MySQL Implementation uses GitHub as issue tracking system to record bugs and feature requests. 
If you want to raise an issue, please follow the recommendations below:

- Before you log a bug, please search the [issue tracker](https://github.com/mirromutth/r2dbc-mysql/issues) to see if someone has already reported the problem.
- If the issue doesn't already exist, [create a new issue](https://github.com/mirromutth/r2dbc-mysql/issues/new).
- Please provide as much information as possible with the issue report, we like to know the version of R2DBC MySQL that you are using and JVM version.
- If you need to paste code, or include a stack trace use Markdown **&#96;&#96;&#96;** escapes before and after your text.
- If possible try to create a test-case or project that replicates the issue. Attach a link to your code or a compressed file containing your code.

## Before use

- MySQL database does **NOT** support **table definition** in prepare statement, please use simple statement if want to execute table definitions.
- Native MySQL data fields encoded by index-based, get fields by index will have **better** performance than get by column name.
- Every `Result` should be used (call `getRowsUpdated` or `map`, even table definition), can **NOT** just ignore any `Result`, otherwise inbound stream is unable to align. (like `ResultSet.close` in jdbc, `Result` auto-close after used by once)
- The MySQL return microseconds when only in prepared statement result (and maybe has not microsecond even in prepared statement result). Therefore this driver does not guarantee time accuracy to microseconds.
- The MySQL database server does not **actively** return time zone when query `DATETIME` or `TIMESTAMP`, this driver does not attempt time zone conversion. That means should always use `LocalDateTime` for SQL type `DATETIME` or `TIMESTAMP`. Execute `SHOW VARIABLES LIKE '%time_zone%'` to get more information.
- Do not turn-on the `trace` log level unless debugging. Otherwise, the security information may be exposed through `ByteBuf` dump.
- If `Statement` bound `returnGeneratedValues`, the `Result` of the `Statement` can be called both: `getRowsUpdated` to get affected rows, and `map` to get last inserted ID.

## License

This project is released under version 2.0 of the [Apache License](https://www.apache.org/licenses/LICENSE-2.0).

[m]: https://www.mysql.com
