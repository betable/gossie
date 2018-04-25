#! /bin/bash -e

go install github.com/golang/mock/mockgen
mockgen github.com/betable/gossie/src/cassandra Cassandra >mock_cassandra/mock_cassandra.go
#mockgen -package="gossie" github.com/betable/gossie/src/gossie ConnectionPool >/tmp/mock_gossie.go
#mv /tmp/mock_gossie.go .
gofmt -w mock_cassandra/mock_cassandra.go

echo >&2 "OK"
