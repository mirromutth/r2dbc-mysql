# Reactive Relational Database Connectivity MySQL Implementation

This project contains the [MySQL][m] implementation of the [R2DBC SPI][s].
This implementation is not intended to be used directly, but rather to be
used as the backing implementation for a humane client library to
delegate to. See [R2DBC Homepage][r].

This driver provides the following features:

- Login with username/password (or no password)
- Execution of simple or batch statements without bindings
- Read text result support for all data types except LOB types (e.g. BLOB, CLOB)
- Transactions (testing, not verify for now)

Next steps:

- Execution of prepared statements with bindings.
- Support LOB types (e.g. BLOB, CLOB)
- Support binary result.
- Complete transactions test.
- TLS (see [#9](https://github.com/mirromutth/r2dbc-mysql/issues/9))

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
ConnectionFactory connectionFactory = new MySqlConnectionFactory(configuration);

// Alternative: Creating a Mono using Project Reactor
Mono<Connection> connectionMono = Mono.from(connectionFactory.create());
```

About more information of `ZeroDateOption`, see **Usage** -> **Discovery**, and should use `enum` in programmatic not like discovery.

### Simple statement

```java
connection.createStatement("INSERT INTO `person` (`first_name`, `last_name`) VALUES ('who', 'how')")
    .execute(); // return a Publisher include one Result
```

Each statement can only contain one query (`SELECT`, `INSERT`, `UPDATE`, etc.) without `;`.

> Every `Result` should be used (call `getRowsUpdated` or `map`, even DDL), can NOT just ignore any `Result`,
> otherwise inbound stream is unable to align. (like `ResultSet.close` in jdbc, `Result` auto-close after used by once)

### Batch statement

```java
connection.createBatch()
    .add("INSERT INTO `person` (`first_name`, `last_name`) VALUES ('who', 'how')")
    .add("UPDATE `earth` SET `count` = `count` + 1 WHERE `id` = 'human'")
    .execute(); // return a Publisher include two Result
```

Each statement of `add(String)` can only contain one query without `;`.

> Every `Result` should be used (call `getRowsUpdated` or `map`, even DDL), can NOT just ignore any `Result`,
> otherwise inbound stream is unable to align. (like `ResultSet.close` in jdbc, `Result` auto-close after used by once)

## Data Type Mapping

This reference table shows the type mapping between [MySQL][m] and Java data types:

| MySQL Type | Unsigned | Support Data Type |
|---|---|---|
| INT | UNSIGNED | `Long` |
| INT | SIGNED | `Integer` |
| TINYINT | UNSIGNED | `Short` |
| TINYINT | SIGNED | `Byte` |
| SMALLINT | UNSIGNED | `Integer` |
| SMALLINT | SIGNED | `Short` |
| MEDIUMINT | SIGNED/UNSIGNED | `Integer` |
| BIGINT | UNSIGNED | `BigInteger`, `Long` (throws `ArithmeticException` if it overflows) |
| BIGINT | SIGNED | `Long` |
| FLOAT | SIGNED/UNSIGNED | `Float` |
| DOUBLE | SIGNED/UNSIGNED | `Double` |
| DECIMAL | SIGNED/UNSIGNED | `BigDecimal`, `Float` (if size less than 7), `Double` (if size less than 16) |
| BIT | - | `BitSet`, `Boolean` (if size is 1), `Byte` (if size less or equals than 8) |
| DATETIME/TIMESTAMP | - | `LocalDateTime` |
| DATE | - | `LocalDate` |
| TIME | - | `LocalTime` |
| YEAR | - | `Integer`, `Year` |
| VARCHAR/NVARCHAR | - | `String` |
| VARBINARY | - | Not support yet |
| BINARY | - | Not support yet |
| CHAR/NCHAR | - | `String` |
| ENUM | - | `String`, `Enum<?>` (Need to pass in a specific enumeration class) |
| SET | - | `String` |
| BLOB (LONGBLOB, etc.) | - | `byte[]` (need change to `Blob`) |
| TEXT (LONGTEXT, etc.) | - | `String` (need change to `Clob`) |
| JSON | - | `String` (see [#15](https://github.com/mirromutth/r2dbc-mysql/issues/15)) |
| GEOMETRY | - | `byte[]` (it should handling same as BLOB, see [#11](https://github.com/mirromutth/r2dbc-mysql/issues/11)) |

## Notice

- The MySQL return microseconds when only in prepared statement result (and maybe has not microsecond even in prepared statement result). Therefore this driver does not guarantee time accuracy to microseconds.
- The MySQL server does not **actively** return time zone when query `DATETIME` or `TIMESTAMP`, this driver does not attempt time zone conversion. That means should always use `LocalDateTime` for SQL type `DATETIME` or `TIMESTAMP`. Execute `SHOW VARIABLES LIKE '%time_zone%'` to get more information.
- Do not turn-on the `trace` log level unless debugging. Otherwise, the security information may be exposed through `ByteBuf` dump.

## License

This project is released under version 2.0 of the [Apache License][l].

[m]: https://www.mysql.com
[s]: https://github.com/r2dbc/r2dbc-spi
[r]: https://r2dbc.io
[l]: https://www.apache.org/licenses/LICENSE-2.0
