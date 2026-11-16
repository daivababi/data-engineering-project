# Ridango MongoDB seed fixture

`ridango_poll_events_2026-11-16T1200-1300_tallinn.ndjson.gz` contains 180
unaltered raw `tartu_bus.ridango_poll_events` documents whose `started_at` is
from 2026-11-16 12:00:00 (inclusive) to 13:00:00 (exclusive), Europe/Tallinn.
All records have `status: "success"`.

The file is line-delimited MongoDB Extended JSON produced by `mongoexport` and
is intended for a future clean-start seed importer. Its SHA-256 is:

```text
dac8daf982de1bf73496b02da76bf83797b4912d36ef46ed207b008554973823
```

The matching GTFS feed must be loaded and dbt dimensions built before this
fixture is cleaned into PostgreSQL observations.

On `docker compose up`, the one-shot `mongo-seed` service imports this file
only when `tartu_bus.ridango_poll_events` is empty. Existing collections are
left unchanged.
