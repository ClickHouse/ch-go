#!/bin/bash

set -e
# Download static build .tgz by version.
# Example: _hack/dl.sh 22.3.2.2-lts
mkdir -p /opt/ch

URL=$(curl -L -s "https://api.github.com/repos/ClickHouse/ClickHouse/releases/tags/v${1}" | grep -o -P "https:\/\/.*clickhouse-common-static-\d.*\.tgz")
echo "Downloading ${1} to /opt/ch: ${URL}"
wget -qO- /tmp/static.tgz "${URL}"| tar -C /opt/ch -v -z --transform 's/\/clickhouse$/clickhouse/' --extract --wildcards "*/bin/clickhouse"

ls -lhsa /opt/ch/clickhouse
