package com.luo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class HbaseCompactor implements AutoCloseable {

    public static final int TIMEOUT = 1000 * 60;
    public static final int MINUTES_TO_MS = 60 * 1000;
    public static final RegionInfoComparator REGION_INFO_COMPARATOR = new RegionInfoComparator();

    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseMgr.class);
    private final Admin hBaseAdmin;
    private final HbaseBatchExecutor batchExecutor;
    private Map<ServerName, List<RegionInfo>> compactingRegions = new TreeMap<>();
    private Map<ServerName, Set<RegionName>> compactedRegions = new HashMap<>();
    private Map<ServerName, Integer> compactIterationCount = new HashMap<>();
    private int compactMinFileCount;
    private int maxCompactingRegionPerServer;

    public HbaseCompactor(int aCompactMinFileCount, int aMaxCompactingRegionPerServer, Admin aHBaseAdmin) throws IOException {
        compactMinFileCount = aCompactMinFileCount;
        maxCompactingRegionPerServer = aMaxCompactingRegionPerServer;
        hBaseAdmin = aHBaseAdmin;
        batchExecutor = new HbaseBatchExecutor(hBaseAdmin);
    }

    public void collectCompactInfo() throws IOException, InterruptedException {

        HTableDescriptor[] hTableDescriptors = hBaseAdmin.listTables();
        Map<RegionName, RegionInfo> regionInfoMap =
            constructInitialRegionInfos(hTableDescriptors);

        ClusterStatus clusterStatus = hBaseAdmin.getClusterStatus();
        Collection<ServerName> servers = clusterStatus.getServers();

        for (ServerName server : servers) {
            if (server != null) {

                List<RegionInfo> regionsOnAServer = new LinkedList<>();

                ServerLoad load = clusterStatus.getLoad(server);
                Map<byte[], RegionLoad> regionsLoad = load.getRegionsLoad();
                for (RegionLoad regionLoad : regionsLoad.values()) {
                    RegionName regionName = new RegionName(regionLoad.getName());
                    RegionInfo regionInfo = regionInfoMap.get(regionName);
                    if (regionInfo != null) {
                        regionsOnAServer.add(regionInfo);
                    }
                }

                List<RegionInfo> compactingRegionsOnAServer =
                    batchExecutor.getCompactingRegions(server, regionsOnAServer);
                compactingRegions.put(server, compactingRegionsOnAServer);
                addToCompactedSet(server, compactingRegionsOnAServer);
            }
        }

        printoutFilteredRegions(compactingRegions);
    }

    public void printOutRegionsPerServer(Configuration conf) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            Admin hBaseAdmin = connection.getAdmin();

            HTableDescriptor[] hTableDescriptors = hBaseAdmin.listTables();
            Map<RegionName, RegionInfo> regionInfos = constructInitialRegionInfos(hTableDescriptors);
            collectRegionMetrics(regionInfos, hBaseAdmin);
            LOGGER.info("total regions: {}", regionInfos.size());
            printoutCompactDetail(filterRegions(regionInfos));
        }
    }

    public void majorCompact(int runTimeInMinute) throws IOException, InterruptedException {
        long startTime = System.currentTimeMillis();
        long stopTime;
        if (runTimeInMinute > 0) {
            stopTime = runTimeInMinute * MINUTES_TO_MS + startTime;
        } else {
            stopTime = Long.MAX_VALUE;
        }

        int loop = 1;
        while (System.currentTimeMillis() < stopTime) {
            LOGGER.info(">>>>>>>> round: {} >>>>>>>>>>>", loop);
            HTableDescriptor[] hTableDescriptors = hBaseAdmin.listTables();

            Map<RegionName, RegionInfo> regionInfos = constructInitialRegionInfos(hTableDescriptors);

            collectRegionMetrics(regionInfos, hBaseAdmin);

            Map<ServerName, List<RegionInfo>> filteredRegions = filterRegions(regionInfos);
            printoutCompactSummary(filteredRegions);

            waitAMinute();

            findNonActiveRegionsAndCompact(hBaseAdmin, filteredRegions);
            loop++;
        }
    }

    private void printoutCompactSummary(Map<ServerName, List<RegionInfo>> aFilteredRegions) {
        for (Map.Entry<ServerName, List<RegionInfo>> entry : aFilteredRegions.entrySet()) {
            Integer count = compactIterationCount.get(entry.getKey());
            if (count == null) {
                count = 0;
            }

            LOGGER.info("To be compacted:{}->{}", entry.getKey() + "(" + count + ")", entry.getValue().size());
        }
    }

    private void printoutCompactDetail(Map<ServerName, List<RegionInfo>> aFilteredRegions) {
        for (Map.Entry<ServerName, List<RegionInfo>> entry : aFilteredRegions.entrySet()) {
            LOGGER.info("To be compacted:{}->{}", entry.getKey(), entry.getValue().size());
            for (RegionInfo regionInfo : entry.getValue()) {
                LOGGER.info("To be compacted:{}->{}", entry.getKey(), regionInfo.getRegionName());
            }
        }
    }

    private void printoutFilteredRegions(Map<ServerName, List<RegionInfo>> aFilteredRegions) {
        for (Map.Entry<ServerName, List<RegionInfo>> entry : aFilteredRegions.entrySet()) {
            for (int i = 0; i < entry.getValue().size(); i++) {
                RegionInfo info = entry.getValue().get(i);
                LOGGER.info("{}->{}", entry.getKey(), info);
            }
        }
    }

    private void findNonActiveRegionsAndCompact(Admin aHBaseAdmin,
                                                Map<ServerName, List<RegionInfo>> aFilteredRegions) throws IOException, InterruptedException {
        ClusterStatus clusterStatus = aHBaseAdmin.getClusterStatus();

        for (ServerName server : clusterStatus.getServers()) {
            int compactingCount = checkCompactingRegions(server, batchExecutor);

            if (compactingCount < maxCompactingRegionPerServer) {
                ServerLoad load = clusterStatus.getLoad(server);
                Map<byte[], RegionLoad> regionsLoad = load.getRegionsLoad();

                List<RegionInfo> regionInfos = aFilteredRegions.get(server);
                for (int i = 0; regionInfos != null && i < regionInfos.size(); i++) {
                    RegionInfo savedInfo = regionInfos.get(i);
                    RegionLoad regionLoad = regionsLoad.get(savedInfo.getRegionName().toByteBinary());
                    if (regionLoad == null) {
                        LOGGER.warn("!!regionLoad doesn't have this region:{}", savedInfo.getRegionName());
                    } else {
                        long requestsCount = regionLoad.getRequestsCount();
                        if (savedInfo.getActivityCount() != requestsCount) {
                            LOGGER.info("Region Busy:{} {}", requestsCount - savedInfo.getActivityCount(),
                                savedInfo.getRegionName());
                        } else {
                            compactingCount = bookKeepingCompactingRegion(server, savedInfo);
                            LOGGER.info("Start Compact:{} with fileCountMinusCF={}", savedInfo.getRegionName(),
                                savedInfo.getFileCountMinusCF());
                            batchExecutor.majorCompact(server, savedInfo.getRegionName());
                            if (compactingCount >= maxCompactingRegionPerServer) {
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    private int bookKeepingCompactingRegion(ServerName aServer, RegionInfo aRegionInfo) {
        addToCompactedSet(aServer, aRegionInfo);

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

    /**
     * @return serverName to list of regions
     */
    private Map<ServerName, List<RegionInfo>> filterRegions(Map<RegionName, RegionInfo> aRegionInfos) {
        HashMap<ServerName, List<RegionInfo>> result = new HashMap<>();
        for (RegionInfo regionInfo : aRegionInfos.values()) {
//            LOGGER.info("add region to server: {}, {}", regionInfo.getServer(),
//                regionInfo.getRegionName().toString() + regionInfo
//                    .isSystemTable() + regionInfo.getFileCountMinusCF() + alreadyCompacted(regionInfo));
            if (!regionInfo.isSystemTable()
                && regionInfo.getFileCountMinusCF() >= compactMinFileCount
                && !alreadyCompacted(regionInfo)) {
                ServerName server = regionInfo.getServer();
                if (server != null) {
                    List<RegionInfo> regionsPerServer = result.get(server);
                    if (regionsPerServer == null) {
                        regionsPerServer = new ArrayList<>();
                        result.put(server, regionsPerServer);
                    }

                    regionsPerServer.add(regionInfo);
                }
            }
        }

        for (List<RegionInfo> regionInfos : result.values()) {
            Collections.sort(regionInfos, REGION_INFO_COMPARATOR);
        }

        //if there is no region to compact, meaning the serverName is not in result, then clear compactedList to
        // start the whole thing over
        for (Map.Entry<ServerName, Set<RegionName>> compactedEntry : compactedRegions.entrySet()) {
            if (!result.containsKey(compactedEntry.getKey())) {
                compactedEntry.getValue().clear();

                increaseIteration(compactedEntry.getKey());
            }
        }
        return result;
    }

    private void increaseIteration(ServerName aKey) {
        Integer count = compactIterationCount.get(aKey);
        if (count == null) {
            compactIterationCount.put(aKey, 1);
        } else {
            compactIterationCount.put(aKey, count + 1);
        }
    }

    private boolean alreadyCompacted(RegionInfo aRegionInfo) {
        Set<RegionName> regions = compactedRegions.get(aRegionInfo.getServer());
        return regions != null && regions.contains(aRegionInfo.getRegionName());
    }

    protected void collectRegionMetrics(Map<RegionName, RegionInfo> aRegionInfos,
                                        Admin hBaseAdmin) throws IOException {
        ClusterStatus clusterStatus = hBaseAdmin.getClusterStatus();

        Collection<ServerName> servers = clusterStatus.getServers();
        for (ServerName server : servers) {
            ServerLoad load = clusterStatus.getLoad(server);
            Map<byte[], RegionLoad> regionsLoad = load.getRegionsLoad();
            for (Map.Entry<byte[], RegionLoad> regionLoadEntry : regionsLoad.entrySet()) {
                RegionLoad regionLoad = regionLoadEntry.getValue();
                RegionInfo regionInfo = aRegionInfos.get(new RegionName(regionLoad.getName()));
                if (regionInfo == null) {
                    LOGGER.error("cannot find regionInfo:{}", regionLoad.getNameAsString());
                }

                if (regionInfo != null && !regionInfo.isSystemTable()) {
                    regionInfo.setFileCount(regionLoad.getStorefiles());
                    regionInfo.setStoreCount(regionLoad.getStores());
                    regionInfo.setActivityCount(regionLoad.getRequestsCount());
                    regionInfo.setServer(server);
                }
            }
        }
    }

    protected Map<RegionName, RegionInfo> constructInitialRegionInfos(HTableDescriptor[] aHTableDescriptors) throws IOException {
        Map<RegionName, RegionInfo> regionInfos = new HashMap<>();

        for (HTableDescriptor tableDescriptor : aHTableDescriptors) {
            TableName tableName = tableDescriptor.getTableName();
            List<HRegionInfo> tableRegions = batchExecutor.getTableRegions(tableName);
            if (tableRegions != null) {
                for (HRegionInfo region : tableRegions) {
                    if (!region.isMetaRegion() && !region.isOffline() && !region.isSplit() && !region
                        .isSplitParent()) {
                        RegionInfo info = new RegionInfo();
                        info.setRegionName(region.getRegionName());
                        info.setTableName(tableName.getNameAsString());
                        info.setSystemTable(region.getTable().isSystemTable());
                        info.setColumnFamilyCount(tableDescriptor.getColumnFamilies().length);
                        regionInfos.put(info.getRegionName(), info);
                    }
                }
            } else {
                LOGGER.warn("cannot find regions for table: {}", tableName);
            }
        }

        return regionInfos;
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
                    LOGGER.info("done compact:{}", next.getRegionName());
                }
            }
            return regionInfos.size();
        } else {
            return 0;
        }
    }

    private void addToCompactedSet(ServerName aServer, List<RegionInfo> aCompacting) {
        Set<RegionName> regions = compactedRegions.get(aServer);
        if (regions == null) {
            regions = new HashSet<>();
            compactedRegions.put(aServer, regions);
        }
        for (RegionInfo compactingRegion : aCompacting) {
            regions.add(compactingRegion.getRegionName());
        }
    }

    private void addToCompactedSet(ServerName aServer, RegionInfo aCompacting) {
        Set<RegionName> regions = compactedRegions.get(aServer);
        if (regions == null) {
            regions = new HashSet<>();
            compactedRegions.put(aServer, regions);
        }
        regions.add(aCompacting.getRegionName());
    }

    @Override
    public void close() throws Exception {
        batchExecutor.close();
    }

    private static class RegionInfoComparator implements Comparator<RegionInfo> {
        @Override
        public int compare(RegionInfo o1, RegionInfo o2) {
            int delta = o2.getFileCountMinusCF() - o1.getFileCountMinusCF();
            if (delta != 0) {
                return delta;
            } else {
                return o2.getRegionName().toString().compareTo(o1.getRegionName().toString());
            }
        }
    }
}
