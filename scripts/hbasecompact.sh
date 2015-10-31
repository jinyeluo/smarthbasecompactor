#!/bin/bash

. ./set_system_env.sh
hadoop jar smarthbasecompactor-1.0.jar com.luo.HbaseMgr -libjars ${HADOOP_LIB_JARS}  -t 300 -f 0 -c 3
