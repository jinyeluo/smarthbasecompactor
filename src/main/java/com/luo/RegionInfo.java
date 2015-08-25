package com.luo;

public class RegionInfo {
    private String name;
    private long activityCount;
    private int fileCount;
    private int storeCount;
    private String tableName;
    private boolean systemTable;
    private String server;
    private int columnFamilyCount;

    public RegionInfo() {
    }

    public String getName() {
        return name;
    }

    public void setName(String aName) {
        name = aName;
    }

    public long getActivityCount() {
        return activityCount;
    }

    public void setActivityCount(long aActivityCount) {
        activityCount = aActivityCount;
    }

    public void setFileCount(int aFileCount) {
        fileCount = aFileCount;
    }

    public void setStoreCount(int aStoreCount) {
        storeCount = aStoreCount;
    }

    public void setTableName(String aTableName) {
        tableName = aTableName;
    }

    public boolean isSystemTable() {
        return systemTable;
    }

    public void setSystemTable(boolean aSystemTable) {
        systemTable = aSystemTable;
    }

    public void setServer(String aServer) {
        server = aServer;
    }

    public String getServer() {
        return server;
    }

    public void setColumnFamilyCount(int aColumnFamilyCount) {
        columnFamilyCount = aColumnFamilyCount;
    }

    public int getFileCountMinusCF() {
        int delta = fileCount - columnFamilyCount;
        if (delta > 0) {
            return delta;
        } else {
            return 0;
        }
    }

    @Override
    public String toString() {
        return "RegionInfo{" +
            "name='" + name + '\'' +
            ", activityCount=" + activityCount +
            ", fileCount=" + fileCount +
            ", storeCount=" + storeCount +
            ", tableName='" + tableName + '\'' +
            ", systemTable=" + systemTable +
            ", server='" + server + '\'' +
            ", columnFamilyCount=" + columnFamilyCount +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RegionInfo that = (RegionInfo) o;

        if (activityCount != that.activityCount) {
            return false;
        }
        if (fileCount != that.fileCount) {
            return false;
        }
        if (storeCount != that.storeCount) {
            return false;
        }
        if (systemTable != that.systemTable) {
            return false;
        }
        if (columnFamilyCount != that.columnFamilyCount) {
            return false;
        }
        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        if (tableName != null ? !tableName.equals(that.tableName) : that.tableName != null) {
            return false;
        }
        return !(server != null ? !server.equals(that.server) : that.server != null);

    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (int) (activityCount ^ (activityCount >>> 32));
        result = 31 * result + fileCount;
        result = 31 * result + storeCount;
        result = 31 * result + (tableName != null ? tableName.hashCode() : 0);
        result = 31 * result + (systemTable ? 1 : 0);
        result = 31 * result + (server != null ? server.hashCode() : 0);
        result = 31 * result + columnFamilyCount;
        return result;
    }
}
