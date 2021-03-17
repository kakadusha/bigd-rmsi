#!/bin/bash
clickhouse-client --port="9000" --host=clickhouse-omsk.rmsi.srv.loc\
 --query="select * from default.metrics_store limit 100"
#
#clickhouse-client --host=clickhouse-omsk.rmsi.srv.loc\
# --user=admin --password=oxoeWie5 --query "select * from default.metrics_store" --format CSVWithNames > metrics_store.csv