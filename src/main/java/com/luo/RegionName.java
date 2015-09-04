/**
 * Copyright(c) 2015 Merkle Inc.  All Rights Reserved.
 * This software is the proprietary information of Merkle Inc.
 */
package com.luo;

import org.apache.hadoop.hbase.util.Bytes;

public class RegionName {
    private String byteName;

    public RegionName(byte[] aByteName) {
        byteName = Bytes.toStringBinary(aByteName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RegionName that = (RegionName) o;

        return !(byteName != null ? !byteName.equals(that.byteName) : that.byteName != null);
    }

    @Override
    public int hashCode() {
        return byteName != null ? byteName.hashCode() : 0;
    }

    @Override
    public String toString() {
        return byteName;
    }

    public byte[] toByteBinary() {
        return Bytes.toBytesBinary(byteName);
    }
}
