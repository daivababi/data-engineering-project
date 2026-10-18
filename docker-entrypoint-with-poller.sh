#!/bin/sh
set -eu

python /app/src/poll_ridango.py &

# Replace this shell with the official PostgreSQL entrypoint. tini runs both
# PostgreSQL and the background poller in one process group and forwards stop
# signals to both of them.
exec docker-entrypoint.sh "$@"
