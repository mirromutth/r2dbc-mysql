# Reactive Relational Database Connectivity MySQL Implementation

This project contains the [MySQL][m] implementation of the [R2DBC SPI][s].
This implementation is not intended to be used directly, but rather to be
used as the backing implementation for a humane client library to
delegate to. See [R2DBC Homepage][r].

This driver provides the following features:

- Login with username/password (or no password)
- Execution of simple or batch statements without bindings
- Read text result support for all data types except LOB types (e.g. BLOB, CLOB)
- Transactions (unverified)

Next steps:

- Execution of prepared statements with bindings.
- Support LOB types (e.g. BLOB, CLOB)
- Support binary result.
- TLS (see [#9](https://github.com/mirromutth/r2dbc-mysql/issues/9))

## Usage

### Programmatic

```java
ConnectProperties options = new ConnectProperties(
    "127.0.0.1", // host
    3306, // port
    Duration.ofSeconds(3), // tcp connect timeout, optional, null means no timeout
    // SSL now not support for now, so it should be false for now
    false, // always use SSL, In the special authentication mode of the MySQL server, it may be forced to use SSL even this value is false
    ZeroDateOption.USE_NULL, // MySQL server maybe return "0000-00-00 00:00:00", This option indicates special handling when MySQL server returning "zero date".
    "root", // username
    "******", // some password, null means has no password
    "r2dbc" // database which connect
);
ConnectionFactory connectionFactory = new MySqlConnectionFactory(ConnectionProvider.newConnection(), options);

// Alternative: Creating a Mono using Project Reactor
Mono<Connection> connectionMono = Mono.from(connectionFactory.create());
```

The above usage is only temporary, the driver may suggest using `Builder` in future versions.

### Simple statement

```java
connection.createStatement("INSERT INTO `person` (`first_name`, `last_name`) VALUES ('who', 'how')")
    .execute(); // return a Publisher include one Result
```

Each statement can only contain one query (`SELECT`, `INSERT`, `UPDATE`, etc.) without `;`.

### Batch statement

```java
connection.createBatch()
    .add("INSERT INTO `person` (`first_name`, `last_name`) VALUES ('who', 'how')")
    .add("UPDATE `earth` SET `count` = `count` + 1 WHERE `id` = 'human'")
    .execute(); // return a Publisher include two Result
```

Each statement of `add(String)` can only contain one query without `;`.

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
| DECIMAL | SIGNED/UNSIGNED | `BigDecimal`, `Float` (if precision less than 7), `Double` (if precision less than 16) |
| BIT | - | `BitSet`, `Boolean` (if precision is 1), `Byte` (if precision less or equals than 8) |
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

- Since the MySQL server does not **actively** return time zone when query `DATETIME` or `TIMESTAMP`, this connector does not attempt time zone conversion. That means should always use `LocalDateTime` for SQL type `DATETIME` or `TIMESTAMP`, and do NOT know what is MySQL connection session time zone. Execute `SHOW VARIABLES LIKE '%time_zone%'` to get more information.
- Do not turn on the `trace` log level unless debugging. Otherwise, the security information may be exposed through `ByteBuf` dump.

## License

This project is released under version 2.0 of the [Apache License][l].

[m]: https://www.mysql.com
[s]: https://github.com/r2dbc/r2dbc-spi
[r]: https://r2dbc.io
[l]: https://www.apache.org/licenses/LICENSE-2.0
