# Reactive Relational Database Connectivity MySQL Implementation

This project contains the [MySQL][m] implementation of the [R2DBC SPI][s].
This implementation is NOT OFFICIAL, official implementations are all in
[R2DBC Homepage][r], but looks like [R2DBC Github Page][g] has no
[MySQL][m] implementation for now (as of January 3, 2019).

**THIS IS ONLY AN EXPERIMENT AND NOT SUPPORTED SOFTWARE FOR NOW**

## Version compatibility

### MySQL

- At least higher than or equal to `5.6.x GA`, low version will not be guaranteed.
- For now, ssl not supported (**I need some help for SSL support, see please**)

## Notice

- Since the MySQL server does not **actively** return time zone when query `DATETIME` or `TIMESTAMP`, this connector does not attempt time zone conversion. That means should always use `LocalDateTime` for SQL type `DATETIME` or `TIMESTAMP`, and do NOT know what is MySQL connection session time zone. Execute `SHOW VARIABLES LIKE '%time_zone%'` to get more information.
- Do not turn on the `trace` log level unless debugging. Otherwise, the security information may be exposed through `ByteBuf` dump.

## License

This project is released under version 2.0 of the [Apache License][l].

[m]: https://www.mysql.com
[s]: https://github.com/r2dbc/r2dbc-spi
[r]: https://r2dbc.io
[g]: https://github.com/r2dbc
[l]: https://www.apache.org/licenses/LICENSE-2.0
