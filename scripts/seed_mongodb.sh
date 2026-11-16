#!/bin/sh

set -eu

if [ ! -f "$MONGO_SEED_FILE" ]; then
    echo "MongoDB seed file not found: $MONGO_SEED_FILE"
    exit 1
fi

mongo_shell() {
    mongosh \
        --host mongodb \
        --port 27017 \
        --username "$MONGO_INITDB_ROOT_USERNAME" \
        --password "$MONGO_INITDB_ROOT_PASSWORD" \
        --authenticationDatabase admin \
        --quiet \
        "$MONGO_DATABASE" \
        "$@"
}

collection_count() {
    mongo_shell --eval 'db.getCollection(process.env.MONGO_COLLECTION).countDocuments({})'
}

expected_count="$(gzip -cd "$MONGO_SEED_FILE" | wc -l | tr -d '[:space:]')"
existing_count="$(collection_count)"

case "$existing_count" in
    ''|*[!0-9]*)
        echo "Could not determine collection count: $existing_count" >&2
        exit 1
        ;;
esac

if [ "$existing_count" -gt 0 ]; then
    echo "Skipped MongoDB seed: $MONGO_DATABASE.$MONGO_COLLECTION already has $existing_count document(s)."
else
    gzip -cd "$MONGO_SEED_FILE" | mongoimport \
        --host mongodb \
        --port 27017 \
        --username "$MONGO_INITDB_ROOT_USERNAME" \
        --password "$MONGO_INITDB_ROOT_PASSWORD" \
        --authenticationDatabase admin \
        --db "$MONGO_DATABASE" \
        --collection "$MONGO_COLLECTION"

    imported_count="$(collection_count)"
    if [ "$imported_count" -ne "$expected_count" ]; then
        echo "MongoDB seed import incomplete: expected $expected_count documents, found $imported_count." >&2
        exit 1
    fi
    echo "Imported $imported_count MongoDB seed document(s)."
fi

mongo_shell --eval '
    const collection = db.getCollection(process.env.MONGO_COLLECTION);
    collection.createIndex({poll_id: 1}, {unique: true});
    collection.createIndex({status: 1, started_at: -1});
'
