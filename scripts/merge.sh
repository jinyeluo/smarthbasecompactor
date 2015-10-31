#!/usr/bin/env bash

. ./set_system_env.sh
hadoop jar ./smarthbasecompactor-1.1-SNAPSHOT.jar com.luo.merge.HBaseMerge2 -libjars ${HADOOP_LIB_JARS} $1 3G
