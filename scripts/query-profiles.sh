#!/bin/sh

echo "Showing cached profiles"
sqlite3 /tmp/profile-entries-map.sqlite 'select * from items;'


echo "Showing cached count of profiles"
sqlite3 /tmp/profile-entries-map.sqlite 'select count(1) from items;'
