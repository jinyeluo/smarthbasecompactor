package com.luo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class HbaseCompactor {

    public static final int TIMEOUT = 1000 * 60;
    public static final int MINTUES_TO_MS = 60 * 1000;
    public static final RegionInfoComparator REGION_INFO_COMPARATOR = new RegionInfoComparator();

    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseMgr.class);

    private Map<ServerName, List<RegionInfo>> compactingRegions = new HashMap<>();

    private int compactMinFileCount;
    private int maxCompactingRegionPerServer;

    public HbaseCompactor(int aCompactMinFileCount, int aMaxCompactingRegionPerServer) {
        compactMinFileCount = aCompactMinFileCount;
        maxCompactingRegionPerServer = aMaxCompactingRegionPerServer;
    }

    public void collectCompactInfo(Configuration conf) throws IOException, InterruptedException {
        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
        HConnection connection = hBaseAdmin.getConnection();
        Map<String, RegionInfo> regionInfoMap =
            constructInitialRegionInfos(hBaseAdmin, connection.listTables());

        ClusterStatus clusterStatus = hBaseAdmin.getClusterStatus();
        Collection<ServerName> servers = clusterStatus.getServers();
        HbaseBatchExecutor executor = null;

        try {
            executor = new HbaseBatchExecutor(hBaseAdmin);

            for (ServerName server : servers) {
                List<RegionInfo> regionsOnAServer = new LinkedList<>();

                ServerLoad load = clusterStatus.getLoad(server);
                Map<byte[], RegionLoad> regionsLoad = load.getRegionsLoad();
                for (RegionLoad regionLoad : regionsLoad.values()) {
                    String regionName = regionLoad.getNameAsString();
                    RegionInfo regionInfo = regionInfoMap.get(regionName);
                    if (regionInfo != null) {
                        regionsOnAServer.add(regionInfo);
                    }
                }

                List<RegionInfo> compactingRegionsOnAServer =
                    executor.getCompactingRegions(server, regionsOnAServer);
                compactingRegions.put(server, compactingRegionsOnAServer);
            }
        } finally {
            if (executor != null) {
                executor.close();
            }
        }

        LOGGER.info("compacting regions:");
        printoutFilteredRegions(compactingRegions);
    }

    public void majorCompact(Configuration conf, int runTimeInMinute) throws IOException, InterruptedException {
        long startTime = System.currentTimeMillis();
        long stopTime;
        if (runTimeInMinute > 0) {
            stopTime = runTimeInMinute * MINTUES_TO_MS + startTime;
        } else {
            stopTime = Long.MAX_VALUE;
        }

        int loop = 1;
        while (System.currentTimeMillis() < stopTime) {
            LOGGER.info(">>>>>>>> round: {} >>>>>>>>>>>", loop);

            HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
            HConnection connection = hBaseAdmin.getConnection();
            HTableDescriptor[] hTableDescriptors = connection.listTables();

            Map<String, RegionInfo> regionInfos = constructInitialRegionInfos(hBaseAdmin, hTableDescriptors);

            collectRegionMetrics(regionInfos, hBaseAdmin);

            Map<String, List<RegionInfo>> filteredRegions = filterRegions(regionInfos);
            printoutCompactSummary(filteredRegions);

            waitAMinute();

            findNonActiveRegionsAndCompact(hBaseAdmin, filteredRegions);
            loop++;
        }
    }

    private void printoutCompactSummary(Map<String, List<RegionInfo>> aFilteredRegions) {
        for (Map.Entry<String, List<RegionInfo>> entry : aFilteredRegions.entrySet()) {
            LOGGER.info("To be compacted:{}->{}", entry.getKey(), entry.getValue().size());
        }
    }

    protected void printoutFilteredRegions(Map<ServerName, List<RegionInfo>> aFilteredRegions) {
        for (Map.Entry<ServerName, List<RegionInfo>> entry : aFilteredRegions.entrySet()) {
            for (int i = 0; i < entry.getValue().size(); i++) {
                RegionInfo info = entry.getValue().get(i);
                LOGGER.info("{}->{}", entry.getKey(), info);
            }
        }
    }

    private void findNonActiveRegionsAndCompact(HBaseAdmin aHBaseAdmin,
        Map<String, List<RegionInfo>> aFilteredRegions) throws IOException, InterruptedException {
        ClusterStatus clusterStatus = aHBaseAdmin.getClusterStatus();

        HbaseBatchExecutor executor = null;
        try {
            executor = new HbaseBatchExecutor(aHBaseAdmin);

            for (ServerName server : clusterStatus.getServers()) {
                int compactingCount = checkCompactingRegions(server, executor);

                if (compactingCount < maxCompactingRegionPerServer) {
                    ServerLoad load = clusterStatus.getLoad(server);
                    Map<byte[], RegionLoad> regionsLoad = load.getRegionsLoad();
                    for (Map.Entry<byte[], RegionLoad> regionLoadEntry : regionsLoad.entrySet()) {
                        RegionLoad regionLoad = regionLoadEntry.getValue();
                        String regionName = regionLoad.getNameAsString();
                        RegionInfo savedInfo = findSavedInfo(server.getServerName(), regionName, aFilteredRegions);
                        if (savedInfo != null) {
                            long requestsCount = regionLoad.getRequestsCount();
                            if (savedInfo.getActivityCount() != requestsCount)
                                LOGGER.info("Region Busy:{} {}", requestsCount - savedInfo.getActivityCount(),
                                    regionName);
                            else {
                                compactingCount = bookKeepingCompactingRegion(server, savedInfo);
                                LOGGER.info("Start Compact:{}", savedInfo.getName());
                                executor.majorCompact(server, savedInfo.getName());
                                if (compactingCount >= maxCompactingRegionPerServer) {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            if (executor != null) {
                executor.close();
            }
        }
    }

    private int bookKeepingCompactingRegion(ServerName aServer, RegionInfo aRegionInfo) {
        List<RegionInfo> regionInfos = compactingRegions.get(aServer);
        if (regionInfos == null) {
            regionInfos = new ArrayList<>();
            compactingRegions.put(aServer, regionInfos);
        }
        regionInfos.add(aRegionInfo);
        return regionInfos.size();
    }

    protected void waitAMinute() {
        try {
            Thread.currentThread().join(TIMEOUT);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private RegionInfo findSavedInfo(String aServerName, String aRegionName,
        Map<String, List<RegionInfo>> aFilteredRegions) {
        List<RegionInfo> regionInfos = aFilteredRegions.get(aServerName);
        for (int i = 0; regionInfos != null && i < regionInfos.size(); i++) {
            RegionInfo regionInfo = regionInfos.get(i);
            if (regionInfo.getName().equals(aRegionName)) {
                return regionInfo;
            }
        }
        return null;
    }

    private Map<String, List<RegionInfo>> filterRegions(Map<String, RegionInfo> aRegionInfos) {
        HashMap<String, List<RegionInfo>> result = new HashMap<>();
        for (RegionInfo regionInfo : aRegionInfos.values()) {
            if (!regionInfo.isSystemTable() && regionInfo.getFileCountMinusCF() >= compactMinFileCount) {
                List<RegionInfo> regionsPerServer = result.get(regionInfo.getServer());
                if (regionsPerServer == null) {
                    regionsPerServer = new ArrayList<>();
                    result.put(regionInfo.getServer(), regionsPerServer);
                    regionsPerServer.add(regionInfo);
                } else {
                    regionsPerServer.add(regionInfo);
                }
            }
        }

        for (List<RegionInfo> regionInfos : result.values()) {
            Collections.sort(regionInfos, REGION_INFO_COMPARATOR);
        }
        return result;
    }

    protected void collectRegionMetrics(Map<String, RegionInfo> aRegionInfos,
        HBaseAdmin hBaseAdmin) throws IOException {
        ClusterStatus clusterStatus = hBaseAdmin.getClusterStatus();

        Collection<ServerName> servers = clusterStatus.getServers();
        for (ServerName server : servers) {
            ServerLoad load = clusterStatus.getLoad(server);
            Map<byte[], RegionLoad> regionsLoad = load.getRegionsLoad();
            for (Map.Entry<byte[], RegionLoad> regionLoadEntry : regionsLoad.entrySet()) {
                RegionLoad regionLoad = regionLoadEntry.getValue();
                RegionInfo regionInfo = aRegionInfos.get(regionLoad.getNameAsString());
                if (regionInfo != null && !regionInfo.isSystemTable()) {
                    regionInfo.setFileCount(regionLoad.getStorefiles());
                    regionInfo.setStoreCount(regionLoad.getStores());
                    regionInfo.setActivityCount(regionLoad.getRequestsCount());
                    regionInfo.setServer(server.getServerName());
                }
            }
        }
    }

    protected Map<String, RegionInfo> constructInitialRegionInfos(HBaseAdmin aHBaseAdmin,
        HTableDescriptor[] aHTableDescriptors) throws IOException {
        Map<String, RegionInfo> regionInfos = new HashMap<>();

        HbaseBatchExecutor executor = null;
        try {
            executor = new HbaseBatchExecutor(aHBaseAdmin);

            for (HTableDescriptor tableDescriptor : aHTableDescriptors) {
                TableName tableName = tableDescriptor.getTableName();
                List<HRegionInfo> tableRegions = executor.getTableRegions(tableName);
                for (HRegionInfo region : tableRegions) {
                    if (!region.isMetaRegion() && !region.isOffline() && !region.isSplit() && !region.isSplitParent()) {
                        RegionInfo info = new RegionInfo();
                        info.setName(region.getRegionNameAsString());
                        info.setTableName(tableName.getNameAsString());
                        info.setSystemTable(region.getTable().isSystemTable());
                        info.setColumnFamilyCount(tableDescriptor.getColumnFamilies().length);
                        regionInfos.put(info.getName(), info);
                    }
                }
            }
        } finally {
            if (executor != null) {
                executor.close();
            }
        }

        return regionInfos;
    }

    private static class RegionInfoComparator implements Comparator<RegionInfo> {
        @Override
        public int compare(RegionInfo o1, RegionInfo o2) {
            int delta = o2.getFileCountMinusCF() - o1.getFileCountMinusCF();
            if (delta != 0) {
                return delta;
            } else {
                return o2.getName().compareTo(o1.getName());
            }
        }
    }

    private int checkCompactingRegions(ServerName server, HbaseBatchExecutor aExecutor)
        throws InterruptedException, IOException {
        List<RegionInfo> regionInfos = compactingRegions.get(server);
        List<RegionInfo> compacting = aExecutor.getCompactingRegions(server, regionInfos);

        if (regionInfos != null) {
            Iterator<RegionInfo> iterator = regionInfos.iterator();
            while (iterator.hasNext()) {
                RegionInfo next = iterator.next();
                if (!compacting.contains(next)) {
                    iterator.remove();
                    LOGGER.info("done compact:{}", next.getName());
                }
            }
            return regionInfos.size();
        } else {
            return 0;
        }
    }

}
