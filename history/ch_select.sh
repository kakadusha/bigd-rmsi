#!/bin/bash
clickhouse-client --host=clickhouse-omsk.rmsi.srv.loc\
 --user=admin --password=oxoeWie5 --query "select * from default.metrics_store" --format CSVWithNames > metrics_store.csv