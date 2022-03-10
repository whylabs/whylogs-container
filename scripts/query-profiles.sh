#!/bin/sh

TMP_DIR=/tmp

echo "Cached profiles:"
sqlite3 $TMP_DIR/dataset-profiles-map-v2.sqlite 'select * from items;'

echo -n "Cached profiles count:"
sqlite3 $TMP_DIR/dataset-profiles-map-v2.sqlite 'select count(1) from items;'


echo -n "Pending request count: "
sqlite3 $TMP_DIR/pending-requests-queue-v2.sqlite 'select count(1) from items;'


