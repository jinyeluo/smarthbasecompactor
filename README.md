# smarthbasecompactor
a smart, automated non-intrusive driver for hbase region-level major-compact. 

It operates on a per server/region level. For every server, it finds regions with most region files, which are the ones that need to be major-compacted the most, then monitor whether the regions have W/R activities. If yes, move to the next region, otherwise issue a major compact command. It also ensure at most it has X number of regions compacting, and X is configurable, so that compact storm wonn't occur.   

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

The code is tested on hbase 0.98. Not sure if it works for earlier version or not.
