#!/bin/sh
set -eu

# Barretenberg locks its CRS directory, so copy the image seed into writable
# runtime storage before starting the read-only sidecar container.
mkdir -p /var/lib/pulsar-verifier/.bb-crs
cp -a /opt/pulsar-verifier/crs/. /var/lib/pulsar-verifier/.bb-crs/

exec /usr/local/bin/pulsar-verifier "$@"
