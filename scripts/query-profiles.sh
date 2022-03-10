#!/bin/sh

# echo "Showing cached profiles"
# sqlite3 /tmp/dataset-profiles-map-v2.sqlite 'select * from items;'

echo -n "Cached profiles count:"
sqlite3 /tmp/dataset-profiles-map-v2.sqlite 'select count(1) from items;'


echo -n "Pending request count: "
sqlite3 /tmp/pending-requests-queue-v2.sqlite 'select count(1) from items;'


