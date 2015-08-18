package com.github.simonthecat.hbasememtest;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.Comparator;

public class BinaryComparator implements Comparator<byte[]> {

    public int compare(byte[] left, byte[] right) {
        return Bytes.compareTo(left, right);
    }

    public boolean isGreater(byte[] left, byte[] right) {
        return compare(left, right) > 0;
    }

    public boolean isLessOrEqual(byte[] left, byte[] right) {
        return !isGreater(left, right);
    }

}