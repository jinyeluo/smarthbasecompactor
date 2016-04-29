#!/bin/bash

. ./set_system_env.sh
yarn jar smarthbasecompactor-1.2.jar com.luo.HbaseMgr -libjars ${HADOOP_LIB_JARS}  -t 600 -f 0 -c 3
