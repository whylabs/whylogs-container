#!/bin/sh

echo "Attempting to restart the rest server through supervisorctl"
supervisorctl -c /opt/whylogs/supervisord.conf restart app
