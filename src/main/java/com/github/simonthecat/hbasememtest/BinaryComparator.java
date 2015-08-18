package com.github.simonthecat.hbasememtest;

import org.apache.hadoop.hbase.filter.CompareFilter;
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
        return compare(left, right) <= 0;
    }

    public boolean isLess(byte[] left, byte[] right) {
        return compare(left, right) < 0;
    }

    public boolean isGreaterOrEqual(byte[] left, byte[] right) {
        return compare(left, right) >= 0;
    }

    public boolean equal(byte[] left, byte[] right) {
        return compare(left, right) == 0;
    }

    public boolean notEqual(byte[] left, byte[] right) {
        return compare(left, right) != 0;
    }

    public boolean byOperator(byte[] left, byte[] right, CompareFilter.CompareOp op) {
        switch (op) {
            case LESS:
                return isLess(left, right);
            case LESS_OR_EQUAL:
                return isLessOrEqual(left, right);
            case EQUAL:
                return equal(left, right);
            case NOT_EQUAL:
                return notEqual(left, right);
            case GREATER_OR_EQUAL:
                return isGreaterOrEqual(left, right);
            case GREATER:
                return isGreater(left, right);
            default:
                throw new RuntimeException("Unknown operator");
        }
    }
}