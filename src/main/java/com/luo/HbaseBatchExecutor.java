/**
 * Copyright(c) 2015 Merkle Inc.  All Rights Reserved.
 * This software is the proprietary information of Merkle Inc.
 */

package com.luo;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState.MAJOR_AND_MINOR_VALUE;
import static org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState.MAJOR_VALUE;

public class HbaseBatchExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseBatchExecutor.class);

    private HBaseAdmin hBaseAdmin;
    private CatalogTracker catTracker;

    public HbaseBatchExecutor(HBaseAdmin aHBaseAdmin) throws IOException {
        hBaseAdmin = aHBaseAdmin;
        synchronized (this) {
            catTracker = null;
            try {
                catTracker = new CatalogTracker(aHBaseAdmin.getConfiguration());
                catTracker.start();
            } catch (InterruptedException e) {
                // Let it out as an IOE for now until we redo all so tolerate IEs
                throw (InterruptedIOException) new InterruptedIOException("Interrupted").initCause(e);
            }
        }
    }

    public List<HRegionInfo> getTableRegions(final TableName tableName)
        throws IOException {
        return MetaReader.getTableRegions(catTracker, tableName, true);
    }

    public void majorCompact(ServerName aServer, RegionName regionName) throws IOException {
        AdminProtos.AdminService.BlockingInterface admin = getAdminFromConnection(aServer);
        AdminProtos.CompactRegionRequest request =
            RequestConverter.buildCompactRegionRequest(regionName.toByteBinary(), true, null);
        try {
            admin.compactRegion(null, request);
        } catch (ServiceException se) {
            LOGGER.warn("exception happened, but process continues", ProtobufUtil.getRemoteException(se));
        }
    }

    private AdminProtos.GetRegionInfoResponse.CompactionState getCompactionState(
        AdminProtos.AdminService.BlockingInterface admin, final byte[] regionName) {
        try {
            AdminProtos.GetRegionInfoRequest request = RequestConverter.buildGetRegionInfoRequest(
                regionName, true);
            AdminProtos.GetRegionInfoResponse response = admin.getRegionInfo(null, request);
            return response.getCompactionState();
        } catch (ServiceException se) {
            LOGGER.warn("got exception, but will continue", ProtobufUtil.getRemoteException(se));
            return null;
        } finally {
            close();
        }
    }

    public void close() {
        catTracker.stop();
    }

    public boolean isCompactingRegion(ServerName aServer, byte[] regionName)
        throws IOException, InterruptedException {
        AdminProtos.AdminService.BlockingInterface admin = getAdminFromConnection(aServer);

        boolean result;
        try {
            AdminProtos.GetRegionInfoResponse.CompactionState compactionState =
                getCompactionState(admin, regionName);

            result = compactionState != null
                && (compactionState.getNumber() == MAJOR_VALUE
                || compactionState.getNumber() == MAJOR_AND_MINOR_VALUE);
        } catch (IllegalArgumentException e) {
            result = true;
            LOGGER.warn("got exception, but will continue", e);
        }
        return result;
    }

    public List<RegionInfo> getCompactingRegions(ServerName aServer, List<RegionInfo> aRegionInfos)
        throws IOException, InterruptedException {
        AdminProtos.AdminService.BlockingInterface admin = getAdminFromConnection(aServer);

        ArrayList<RegionInfo> results = new ArrayList<>();
        if (aRegionInfos != null) {
            for (RegionInfo regionInfo : aRegionInfos) {
                boolean result;
                try {
                    AdminProtos.GetRegionInfoResponse.CompactionState compactionState =
                        getCompactionState(admin, regionInfo.getRegionName().toByteBinary());

                    result = compactionState != null
                        && (compactionState.getNumber() == MAJOR_VALUE
                        || compactionState.getNumber() == MAJOR_AND_MINOR_VALUE);
                } catch (IllegalArgumentException e) {
                    result = false;
                    LOGGER.warn("got exception, but will continue", e);
                }
                if (result) {
                    results.add(regionInfo);
                }
            }
        }

        return results;
    }

    protected AdminProtos.AdminService.BlockingInterface getAdminFromConnection(ServerName aServer) throws IOException {
        return hBaseAdmin.getConnection().getAdmin(aServer, false);
    }

}
