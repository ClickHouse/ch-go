#!/bin/bash

# Pipe generated native data to clickhouse-local and check that the number of rows is correct.
go run ./internal/cmd/ch-native-dump | clickhouse local \
 --structure "event Enum8('WatchEvent'=1, 'PushEvent'=2, 'IssuesEvent'=3, 'PullRequestEvent'=4), repo Int64, actor Int64, time DateTime" \
 --input-format Native \
  -q "SELECT count() FROM table"

# Result should be 10000.
