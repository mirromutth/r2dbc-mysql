# Reactive Relational Database Connectivity MySQL Implementation

This project contains the [MySQL][m] implementation of the [R2DBC SPI][s].
This implementation is not intended to be used directly, but rather to be
used as the backing implementation for a humane client library to
delegate to. See [R2DBC Homepage][r].

This driver provides the following features:

- Login with username/password (or no password)
- Execution of simple or batch statements without bindings
- Execution of prepared statements with bindings.
- Support LOB types (e.g. BLOB, CLOB)
- Support text/binary result.
- Native ping command.
- Support all charsets from MySQL, like `utf8mb4_0900_ai_ci`, `latin1_general_ci`, `utf32_unicode_520_ci`, etc.
- Transactions (testing, not verify for now)

Next steps:

- Support all exceptions of error code mapping.
- TLS (see [#9](https://github.com/mirromutth/r2dbc-mysql/issues/9))
- Add more unit tests.
- Complete transaction tests.

## Usage

### Discovery

```java
ConnectionFactoryOptions options = ConnectionFactoryOptions.builder()
    .option(DRIVER, "mysql")
    .option(HOST, "127.0.0.1")
    .option(USER, "root")
    .option(PORT, 3306)  // optional, default 3306
    .option(CONNECT_TIMEOUT, Duration.ofSeconds(3)) // optional, default null, null means no timeout
    .option(SSL, false) // optional, default is disabled. SSL not support, it should be disable for now
    .option(Option.valueOf("zeroDate"), "use_null") // optional, default "use_null".
    .option(PASSWORD, "some-password-in-here") // optional, default null, null means has no password
    .option(DATABASE, "r2dbc") // optional, default null, null means not specifying the database
    .build();
ConnectionFactory connectionFactory = ConnectionFactories.get(options);

// Alternative: Creating a Mono using Project Reactor
Mono<Connection> connectionMono = Mono.from(connectionFactory.create());
```

The `zeroDate` indicates special handling when MySQL server returning "zero date" (i.e. `0000-00-00 00:00:00`),
it has three option and ignore case:

- `"exception"`: Just throw a exception when MySQL server return "zero date".
- `"use_null"`: Use `null` when MySQL server return "zero date".
- `"use_round"`: **NOT** RECOMMENDED, only for compatibility. Use "round" date (i.e. `0001-01-01 00:00:00`) when MySQL server return "zero date".

### Programmatic

```java
MySqlConnectConfiguration configuration = MySqlConnectConfiguration.builder()
    .host("127.0.0.1")
    .port(3306) // optional, default 3306
    .connectTimeout(Duration.ofSeconds(3)) // optional, default null, null means no timeout
    .disableSsl() // optional, default is disabled. SSL not support, it should be disable for now
    .zeroDateOption(ZeroDateOption.USE_NULL) // optional, default USE_NULL
    .username("root")
    .password("some password in here") // optional, default null, null means has no password
    .database("r2dbc") // optional, default null, null means not specifying the database
    .build();
ConnectionFactory connectionFactory = MySqlConnectionFactory.from(configuration);

// Alternative: Creating a Mono using Project Reactor
Mono<Connection> connectionMono = Mono.from(connectionFactory.create());
```

About more information of `ZeroDateOption`, see **Usage** -> **Discovery**, and should use `enum` in programmatic not like discovery.

### Simple statement

```java
connection.createStatement("INSERT INTO `person` (`first_name`, `last_name`) VALUES ('who', 'how')")
    .execute(); // return a Publisher include one Result
```

Each statement can only contain one query (`SELECT`, `INSERT`, `UPDATE`, etc.).

- MySQL does **NOT** support **table definition** in prepare statement, please use simple statement if want to execute table definitions.
- Every `Result` should be used (call `getRowsUpdated` or `map`, even table definition), can NOT just ignore any `Result`, otherwise inbound stream is unable to align. (like `ResultSet.close` in jdbc, `Result` auto-close after used by once)
- If bound `returnGeneratedValues`, call `getRowsUpdated` to get affected rows, call `map` to get last inserted ID, can be called both.

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

Each statement can only contain one query (`SELECT`, `INSERT`, `UPDATE`, etc.).

- All parameters must be bound before execute, even parameter is `null` (use `bindNull` to bind `null`).
- In one-to-one binding, because native MySQL prepared statements use index-based parameters, *index-bindings* will have **better** performance than *name-bindings*.
- Native MySQL data rows encoded by index-based, get fields by index will have **better** performance than get by column name.
- MySQL does **NOT** support **table definition** in prepare statement, please use simple statement if want to execute table definitions.
- Every `Result` should be used (call `getRowsUpdated` or `map`), can NOT just ignore any `Result`, otherwise inbound stream is unable to align. (like `ResultSet.close` in jdbc, `Result` auto-close after used by once)
- If bound `returnGeneratedValues`, call `getRowsUpdated` to get affected rows, call `map` to get last inserted ID, can be called both.

### Batch statement

```java
connection.createBatch()
    .add("INSERT INTO `person` (`first_name`, `last_name`) VALUES ('who', 'how')")
    .add("UPDATE `earth` SET `count` = `count` + 1 WHERE `id` = 'human'")
    .execute(); // return a Publisher include two Result
```

Each statement of `add(String)` can only contain one query, `;` will be removed if has only whitespace follow the `;`.

> Every `Result` should be used (call `getRowsUpdated` or `map`, even table definition), can NOT just ignore any `Result`,
> otherwise inbound stream is unable to align. (like `ResultSet.close` in jdbc, `Result` auto-close after used by once)

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
| TIME | - | `LocalTime` |
| YEAR | - | `Short`, `Integer`, `Long`, `BigInteger`, `Year` |
| VARCHAR/NVARCHAR | - | `String` |
| CHAR/NCHAR | - | `String` |
| ENUM | - | `String`, `Enum<?>` |
| SET | - | `String[]`, `String`, `Set<String>` and `Set<Enum<?>>` (`Set<T>` need use `ParameterizedType`) |
| BLOB (LONGBLOB, etc.) | - | `Blob`, `byte[]` (Not check overflow) |
| TEXT (LONGTEXT, etc.) | - | `Clob`, `String` (Not check overflow) |
| JSON | - | `String`, `Clob` |
| GEOMETRY | - | `byte[]`, `Blob` |

## Notice

- The MySQL return microseconds when only in prepared statement result (and maybe has not microsecond even in prepared statement result). Therefore this driver does not guarantee time accuracy to microseconds.
- The MySQL server does not **actively** return time zone when query `DATETIME` or `TIMESTAMP`, this driver does not attempt time zone conversion. That means should always use `LocalDateTime` for SQL type `DATETIME` or `TIMESTAMP`. Execute `SHOW VARIABLES LIKE '%time_zone%'` to get more information.
- Do not turn-on the `trace` log level unless debugging. Otherwise, the security information may be exposed through `ByteBuf` dump.
- Client default charset is `utf8mb4_general_ci`, will not change table definitions or schema (i.e. database) definitions.

## License

This project is released under version 2.0 of the [Apache License][l].

[m]: https://www.mysql.com
[s]: https://github.com/r2dbc/r2dbc-spi
[r]: https://r2dbc.io
[l]: https://www.apache.org/licenses/LICENSE-2.0
