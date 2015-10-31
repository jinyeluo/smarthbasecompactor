/**
 * Copyright(c) 2015 Merkle Inc.  All Rights Reserved.
 * This software is the proprietary information of Merkle Inc.
 */

package com.luo.merge;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

class MergeRegionInfo {
    private final TableName tableName;
    private final String regionNameAsString;
    private final byte[] encodedName;
    private byte[] name;
    private byte[] startKey;
    private byte[] endKey;
    private ServerName serverName;
    private int sizeMB = -1;
    private long activityCount;
    private boolean merged = false;

    public MergeRegionInfo(HRegionInfo aTableRegion) {
        name = aTableRegion.getRegionName();
        tableName = aTableRegion.getTable();
        startKey = aTableRegion.getStartKey();
        endKey = aTableRegion.getEndKey();
        regionNameAsString = aTableRegion.getRegionNameAsString();
        encodedName = aTableRegion.getEncodedNameAsBytes();
    }

    static public MergeRegionInfo find(List<MergeRegionInfo> infos, byte[] regionName) {
        for (int i = 0; i < infos.size(); i++) {
            MergeRegionInfo mergeRegionInfo = infos.get(i);
            if (Bytes.equals(mergeRegionInfo.name, regionName)) {
                return mergeRegionInfo;
            }
        }
        return null;
    }

    @Override
    public int hashCode() {
        return Bytes.hashCode(name);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof MergeRegionInfo && Bytes.equals(name, ((MergeRegionInfo) obj).name);
    }

    public TableName getTableName() {
        return tableName;
    }

    public byte[] getEncodedName() {
        return encodedName;
    }

    public ServerName getServerName() {
        return serverName;
    }

    public void setServerName(ServerName aServerName) {
        serverName = aServerName;
    }

    public byte[] getName() {
        return name;
    }

    public byte[] getStartKey() {
        return startKey;
    }

    public byte[] getEndKey() {
        return endKey;
    }

    public Object getNameAsStr() {
        return regionNameAsString;
    }

    public int getSizeMB() {
        return sizeMB;
    }

    public void setSizeMB(int aSizeMB) {
        sizeMB = aSizeMB;
    }

    public long getActivityCount() {
        return activityCount;
    }

    public void setActivityCount(long aActivityCount) {
        activityCount = aActivityCount;
    }

    public boolean isMerged() {
        return merged;
    }

    public void setMerged(boolean aMerged) {
        merged = aMerged;
    }
}
