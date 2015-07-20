# smarthbasecompactor
a smart, automated non-intrusive driver for hbase region-level major-compact

Parameters:
 -c,--serverConcurrency <minFileThreshold>   max # of regions will be
                                             compacted at the same time
                                             per server, default to 2
 -f,--minFileThreshold <minFileThreshold>    any region with >= fileCount
                                             will be compact candidate,
                                             defaulted to 2
 -t,--runPeriod <runPeriod>                  # of minutes it should run,
                                             30 for half an hour and -1
                                             for forever

Sample:
  hadoop jar smarthbasecompactor-1.0-SNAPSHOT.jar com.luo.HbaseMgr -libjars ${HADOOP_LIB_JARS} -t 30 -f 2 -c 2