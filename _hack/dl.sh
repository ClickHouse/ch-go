#!/bin/bash

set -e
# Download static build .tgz by version.
# Example: _hack/dl.sh 22.3.2.2-lts

URL=$(curl -L -s "https://api.github.com/repos/ClickHouse/ClickHouse/releases/tags/v${1}" | grep -o -P "https:\/\/.*clickhouse-common-static-\d.*\.tgz")
echo "Downloading ${1}: ${URL}"
wget -O /tmp/static.tgz "${URL}"

tar -xvf /tmp/static.tgz

mkdir -p /opt/ch
tar -C /opt/ch -v --strip-components 4 --extract --file /tmp/static.tgz --wildcards "*/bin/clickhouse"
ls -lhsa /opt/ch/clickhouse

rm /tmp/static.tgz
