package com.github.simonthecat.hbasememtest;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class DataUtil {

    private static BinaryComparator BINARY_COMPARATOR = new BinaryComparator();

    public static NavigableMap<byte[], NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> fromMap(Map<byte[], Map<byte[], Map<byte[], Map<Long, byte[]>>>> src) {
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> dst = new TreeMap<>(BINARY_COMPARATOR);

        for (byte[] rowkey : src.keySet()) {
            for (byte[] family : src.get(rowkey).keySet()) {
                for (byte[] qualifier : src.get(rowkey).get(family).keySet()) {
                    for (Long ts : src.get(rowkey).get(family).get(qualifier).keySet()) {
                        byte[] value = src.get(rowkey).get(family).get(qualifier).get(ts);

                        if (!dst.containsKey(rowkey)) {
                            dst.put(rowkey, new TreeMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>(BINARY_COMPARATOR));
                        }

                        if (!dst.get(rowkey).containsKey(family)) {
                            dst.get(rowkey).put(family, new TreeMap<byte[], NavigableMap<Long, byte[]>>(BINARY_COMPARATOR));
                        }

                        if (!dst.get(rowkey).get(family).containsKey(qualifier)) {
                            dst.get(rowkey).get(family).put(qualifier, new TreeMap<Long, byte[]>());
                        }

                        dst.get(rowkey).get(family).get(qualifier).put(ts, value);
                    }
                }
            }
        }

        return dst;
    }

    public static NavigableMap<byte[], NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> fromStringMap(Map<String, Map<String, Map<String, Map<Long, String>>>> src) {
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> dst = new TreeMap<>(BINARY_COMPARATOR);

        for (String rowKeyStr : src.keySet()) {
            for (String familyStr : src.get(rowKeyStr).keySet()) {
                for (String qualifierStr : src.get(rowKeyStr).get(familyStr).keySet()) {
                    for (Long ts : src.get(rowKeyStr).get(familyStr).get(qualifierStr).keySet()) {
                        byte[] value = src.get(rowKeyStr).get(familyStr).get(qualifierStr).get(ts).getBytes();
                        byte[] rowKey = rowKeyStr.getBytes();
                        byte[] family = familyStr.getBytes();
                        byte[] qualifier = qualifierStr.getBytes();

                        if (!dst.containsKey(rowKey)) {
                            dst.put(rowKey, new TreeMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>(BINARY_COMPARATOR));
                        }

                        if (!dst.get(rowKey).containsKey(family)) {
                            dst.get(rowKey).put(family, new TreeMap<byte[], NavigableMap<Long, byte[]>>(BINARY_COMPARATOR));
                        }

                        if (!dst.get(rowKey).get(family).containsKey(qualifier)) {
                            dst.get(rowKey).get(family).put(qualifier, new TreeMap<Long, byte[]>());
                        }

                        dst.get(rowKey).get(family).get(qualifier).put(ts, value);
                    }
                }
            }
        }

        return dst;
    }
}
