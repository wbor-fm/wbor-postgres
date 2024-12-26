# wbor-postgres

Postgres handler - RabbitMQ consumer to make insertions, etc.

## TODO

- Fix termination - if the connection to MQ fails due to "CONNECTION_FORCED - Closed via management plugin", don't re-spawn a consumer
