#!/bin/bash -x
# run_partition.sh <partition>
###############################
#    engine = Kafka SETTINGS kafka_broker_list = 'kafka-0-voronezh.rmsi.srv.loc:9092,kafka-1-voronezh.rmsi.srv.loc:9092,kafka-2-voronezh.rmsi.srv.loc:9092',
#    kafka_topic_list = 'metrics', 
#    kafka_group_name = 'clickHouse', 
#    kafka_format = 'JSONEachRow', 
#    kafka_num_consumers = 2, 
#    kafka_skip_broken_messages = 10;
###############################
OUT="metrics${1}_digtest_`date '+%Y%m%d%H%M%S'`.tsv"
HIVEDEST="/tmp/shm_test_netflow"
KAFKACATPATH="/home/ansible/kafkacat/"
#KAFKACATPATH=""
KAFKABROCKERS="kafka-0-voronezh.rmsi.srv.loc:9092,kafka-1-voronezh.rmsi.srv.loc:9092,kafka-2-voronezh.rmsi.srv.loc:9092"
KAFKATOPIC="metrics"
KAFKAGROUP="bigdata-rmsi-test"
#KAFKACAT_OFFS_OPT="-o -10000"
#KAFKACAT_OFFS_OPT="-o -4000000000"
#KAFKACAT_OFFS_OPT="-o stored"
#KAFKACAT_OFFS_OPT="-o end"
KAFKACAT_OFFS_OPT="-o beginning"
KAFKASASL="-X security.protocol=SASL_PLAINTEXT -X sasl.mechanism=PLAIN -X sasl.username=metrics -X sasl.password=ORaeD8th"
#KAFKACAT_OFF_STOR="-X topic.auto.commit.interval.ms=1000 -X topic.offset.store.path=/tmp/shm-offsets"
#

#${KAFKACATPATH}kafkacat -C -b $KAFKABROCKERS -t $KAFKATOPIC -p $1 $KAFKACAT_OFFS_OPT -G shm -e
${KAFKACATPATH}kafkacat -b $KAFKABROCKERS -G $KAFKAGROUP $KAFKASASL $KAFKATOPIC -p $1 $KAFKACAT_OFFS_OPT -e > $OUT
