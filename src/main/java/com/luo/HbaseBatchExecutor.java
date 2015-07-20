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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState
    .MAJOR_AND_MINOR_VALUE;
import static org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState.MAJOR_VALUE;

public class HbaseBatchExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseBatchExecutor.class);

    public static final Charset UTF_8 = Charset.forName("UTF-8");
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

    public void majorCompact(ServerName aServer, String regionName) throws IOException {
        AdminProtos.AdminService.BlockingInterface admin = getAdminFromConnection(aServer);
        AdminProtos.CompactRegionRequest request =
            RequestConverter.buildCompactRegionRequest(regionName.getBytes(UTF_8), true, null);
        try {
            admin.compactRegion(null, request);
        } catch (ServiceException se) {
            throw ProtobufUtil.getRemoteException(se);
        }
    }

    private AdminProtos.GetRegionInfoResponse.CompactionState getCompactionState(
        AdminProtos.AdminService.BlockingInterface admin, final String regionName) {
        try {
            AdminProtos.GetRegionInfoRequest request = RequestConverter.buildGetRegionInfoRequest(
                regionName.getBytes(UTF_8), true);
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

    public List<RegionInfo> getCompactingRegions(ServerName aServer, List<RegionInfo> aRegionInfos)
        throws IOException, InterruptedException {
        AdminProtos.AdminService.BlockingInterface admin = getAdminFromConnection(aServer);

        ArrayList<RegionInfo> results = new ArrayList<>();
        if (aRegionInfos != null) {
            for (RegionInfo regionInfo : aRegionInfos) {
                boolean result;
                try {
                    AdminProtos.GetRegionInfoResponse.CompactionState compactionState =
                        getCompactionState(admin, regionInfo.getName());

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
