/**
 * Copyright(c) 2015 Merkle Inc.  All Rights Reserved.
 * This software is the proprietary information of Merkle Inc.
 */

package com.luo.merge;

import com.google.protobuf.ServiceException;
import com.luo.HbaseBatchExecutor;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class HBaseMerge2 extends Configured implements Tool {
    public static final int K = 1024;
    public static final int HALF_MIN = 1000 * 30;
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseMerge2.class);

    protected HBaseMerge2() {
    }

    public static void main(String[] args) throws Exception {
        LOGGER.info(">>>>> enters main with params:" + ArrayUtils.toString(args));
        int exitCode = ToolRunner.run(new HBaseMerge2(), args);
        LOGGER.info(">>>>> ready to exit ");
        System.exit(exitCode);
    }

    private static void printUsage() {
        System.out.println("Usage: HBaseMerge $TableName Size (Size can be '100M', '1G' or '10G'");
        System.exit(1);
    }

    @Override
    public int run(String[] args) throws Exception {
        setConf(HBaseConfiguration.create(getConf()));

        if (args.length < 2) {
            printUsage();
        }

        long limitMB = 0;
        String sizeStr = args[1].substring(0, args[1].length() - 1);
        int size = Integer.parseInt(sizeStr);
        switch (args[1].charAt(args[1].length() - 1)) {
            case 'M':
                limitMB = size;
                break;
            case 'G':
                limitMB = size * K;
                break;
            default:
                printUsage();
                break;
        }

        HBaseAdmin admin = new HBaseAdmin(getConf());
        String tableName = args[0];
        List<MergeRegionInfo> regions = getRegions(admin, TableName.valueOf(tableName));
        fillRegionServer(admin, regions);
        Collection<ServerName> serverNames = fillRegionSize(admin, regions);

        boolean smallTable = regions.size() < serverNames.size();
        if (!hasAnyActivity(admin, regions)) {
            filterAndMerge(admin, regions, limitMB, smallTable);
        } else {
            LOGGER.info("activity on the table found, do it the next time: {}", tableName);
        }
        return 0;
    }

    List<MergeRegionInfo> getRegions(HBaseAdmin aAdmin, TableName aTableName)
        throws IOException, InterruptedException {
        List<HRegionInfo> tableRegions = aAdmin.getTableRegions(aTableName);
        List<MergeRegionInfo> mergeRegionInfos = new ArrayList<>();
        for (HRegionInfo tableRegion : tableRegions) {
            if (!tableRegion.isMetaTable() && !tableRegion.isOffline() && !tableRegion.isSplit()
                && !tableRegion.isSplitParent()) {
                mergeRegionInfos.add(new MergeRegionInfo(tableRegion));
            }
        }
        return mergeRegionInfos;
    }

    private void fillRegionServer(HBaseAdmin aAdmin, List<MergeRegionInfo> aRegions) {

        HConnection connection = aAdmin.getConnection();
        Iterator<MergeRegionInfo> iterator = aRegions.iterator();
        while (iterator.hasNext()) {
            MergeRegionInfo region = iterator.next();
            try {
                HRegionLocation hRegionLocation = connection.locateRegion(region.getName());
                region.setServerName(hRegionLocation.getServerName());
            } catch (IOException e) {
                iterator.remove();
                LOGGER.warn("region cannot be found, move on.", e);
            }
        }
    }


    void filterAndMerge(HBaseAdmin aAdmin, List<MergeRegionInfo> aRegions, long aLimitMB, boolean aSmallTable)
        throws IOException, InterruptedException, ServiceException {
        HConnection connection = aAdmin.getConnection();
        MasterProtos.MasterService.BlockingInterface master = connection.getMaster();

        Iterator<MergeRegionInfo> iterator = aRegions.iterator();
        int mergedCount = 0;
        HbaseBatchExecutor batchExecutor = new HbaseBatchExecutor(aAdmin);
        while (iterator.hasNext()) {
            MergeRegionInfo region = iterator.next();
            iterator.remove();

            if (region.isMerged()) {
//                LOGGER.info("region size too big:{} {}MB", region.getNameAsStr(), region.getSizeMB());
            } else if (region.getSizeMB() > aLimitMB) {
                LOGGER.info("region size too big:{} {}MB", region.getNameAsStr(), region.getSizeMB());
            } else if (aSmallTable && region.getSizeMB() > 1) {
                LOGGER.info("skip non-empty region for small table:", region.getNameAsStr(), region.getSizeMB());
            } else {
                MergeRegionInfo nextRegion = getAdjacent(region, aRegions);
                if (nextRegion == null) {
                    LOGGER.info("no adjancent region found:{}", region.getNameAsStr());
                } else if (nextRegion.getSizeMB() + region.getSizeMB() >= aLimitMB) {
                    LOGGER.info("size too big after merge:{} {}MB", region.getNameAsStr(),
                        nextRegion.getSizeMB() + region.getSizeMB());
                } else if (batchExecutor.isCompactingRegion(region.getServerName(), region.getName())
                    || batchExecutor.isCompactingRegion(nextRegion.getServerName(), nextRegion.getName())) {
                    LOGGER.info("one of region is compacting, skip {} --- {}", region.getNameAsStr(),
                        nextRegion.getNameAsStr());
                    nextRegion.setMerged(true);
                } else {
                    LOGGER.info("merge {} -- {}", region.getNameAsStr(), nextRegion.getNameAsStr());
                    byte[] encodedName1 = region.getEncodedName();
                    byte[] encodedName2 = nextRegion.getEncodedName();
                    mergeRegions(master, encodedName1, encodedName2, false);


                    nextRegion.setMerged(true);
                    mergedCount++;
                }
            }
        }

        LOGGER.info("merged count:{}", mergedCount);
    }

    private void mergeRegions(MasterProtos.MasterService.BlockingInterface masterService,
                              final byte[] encodedNameOfRegionA,
                              final byte[] encodedNameOfRegionB, final boolean forcible)
        throws IOException, ServiceException {
        try {
            MasterProtos.DispatchMergingRegionsRequest request = RequestConverter
                .buildDispatchMergingRegionsRequest(encodedNameOfRegionA,
                    encodedNameOfRegionB, forcible);
            masterService.dispatchMergingRegions(null, request);
        } catch (DeserializationException de) {
            LOGGER.error("Could not parse destination server name: " + de);
        }
    }

    private MergeRegionInfo getAdjacent(MergeRegionInfo aRegion, List<MergeRegionInfo> aRegions) {
        for (MergeRegionInfo testRegion : aRegions) {
            if (Bytes.equals(aRegion.getStartKey(), testRegion.getEndKey())
                || Bytes.equals(aRegion.getEndKey(), testRegion.getStartKey())) {
                return testRegion;
            }
        }
        return null;
    }

    private Collection<ServerName> fillRegionSize(HBaseAdmin aAdmin, List<MergeRegionInfo> infos) {
        Collection<ServerName> servers;
        try {
            final ClusterStatus clusterStatus = aAdmin.getClusterStatus();

            servers = clusterStatus.getServers();
            for (ServerName serverName : servers) {
                final ServerLoad serverLoad = clusterStatus.getLoad(serverName);

                for (Map.Entry<byte[], RegionLoad> entry : serverLoad.getRegionsLoad().entrySet()) {

                    MergeRegionInfo mergeRegionInfo = MergeRegionInfo.find(infos, entry.getKey());
                    if (mergeRegionInfo != null) {
                        RegionLoad regionLoad = entry.getValue();
                        mergeRegionInfo.setSizeMB(regionLoad.getStorefileSizeMB());
                        mergeRegionInfo.setActivityCount(regionLoad.getRequestsCount() + regionLoad
                            .getWriteRequestsCount());
                    }
                }
            }
        } catch (IOException e) {
            LOGGER.warn("getClusterStatus failed", e);
            infos.clear();
            servers = new ArrayList<>();
        }

        Iterator<MergeRegionInfo> iterator = infos.iterator();
        while (iterator.hasNext()) {
            MergeRegionInfo regionInfo = iterator.next();
            if (regionInfo.getSizeMB() < 0) {
                iterator.remove();
                LOGGER.warn("region skipped because size cannot be found: {}", regionInfo.getNameAsStr());
            }
        }

        return servers;
    }

    protected void waitForAWhile() {
        try {
            Thread.currentThread().join(HALF_MIN);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean hasAnyActivity(HBaseAdmin aAdmin, List<MergeRegionInfo> infos) {
        waitForAWhile();
        Collection<ServerName> servers;
        try {
            final ClusterStatus clusterStatus = aAdmin.getClusterStatus();
            servers = clusterStatus.getServers();
            for (ServerName serverName : servers) {
                final ServerLoad serverLoad = clusterStatus.getLoad(serverName);

                for (Map.Entry<byte[], RegionLoad> entry : serverLoad.getRegionsLoad().entrySet()) {

                    MergeRegionInfo mergeRegionInfo = MergeRegionInfo.find(infos, entry.getKey());
                    if (mergeRegionInfo != null) {
                        RegionLoad regionLoad = entry.getValue();
                        long currentActivityCount = regionLoad.getRequestsCount() + regionLoad
                            .getWriteRequestsCount();
                        if (currentActivityCount != mergeRegionInfo.getActivityCount()) {
                            return true;
                        }
                    }
                }
            }
        } catch (IOException e) {
            return true;
        }

        return false;
    }
}
