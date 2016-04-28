#!/bin/sh
export USER_LIB=/usr/hdp/current
export HBASE_CONFIG=/etc/hbase/conf

export HBASE_COMMON_JAR=${USER_LIB}/hbase-client/lib/hbase-common.jar
export HBASE_CLIENT_JAR=${USER_LIB}/hbase-client/lib/hbase-client.jar
export HBASE_PROTOCOL_JAR=${USER_LIB}/hbase-client/lib/hbase-protocol.jar

export HADOOP_CLASSPATH=${HBASE_CONFIG}:$HBASE_COMMON_JAR:$HBASE_CLIENT_JAR:$HBASE_PROTOCOL_JAR
export HADOOP_LIB_JARS=${HBASE_CONFIG},$HBASE_COMMON_JAR,$HBASE_CLIENT_JAR,$HBASE_PROTOCOL_JAR
