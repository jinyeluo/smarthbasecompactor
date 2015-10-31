#!/bin/sh
export USER_LIB=/usr/hdp/current
export HBASE_CONFIG=/etc/hbase/conf

export HADOOP_CLASSPATH=${HBASE_CONFIG}
export HADOOP_LIB_JARS=${HBASE_CONFIG}

for f in ${USER_LIB}/hbase-client/lib/*.jar;do
    export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:$f;
    export HADOOP_LIB_JARS=${HADOOP_LIB_JARS},$f;
done

