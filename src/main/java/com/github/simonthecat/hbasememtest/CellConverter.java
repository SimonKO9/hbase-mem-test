package com.github.simonthecat.hbasememtest;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

public class CellConverter {

    public CellConverter() {
    }

    public List<Cell> toCellList(byte[] rowKey, NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> valueMap) {
        List<Cell> keyValues = new ArrayList<Cell>();

        for (byte[] family : valueMap.keySet()) {
            for (byte[] qualifier : valueMap.get(family).keySet()) {
                for (Long timestamp : valueMap.get(family).get(qualifier).keySet()) {
                    byte[] value = valueMap.get(family).get(qualifier).get(timestamp);
                    keyValues.add(new KeyValue(rowKey, family, qualifier, timestamp, value));
                }
            }
        }

        return keyValues;
    }

    public Cell toCell(byte[] rowKey, byte[] family, byte[] qualifier, Long timestamp, byte[] value) {
        return new KeyValue(rowKey, family, qualifier, timestamp, value);
    }

    public List<Cell> toCellList(byte[] rowKey, byte[] family, byte[] qualifier, NavigableMap<Long, byte[]> timestampToValues) {
        List<Cell> cells = new ArrayList<Cell>(timestampToValues.size());
        for (Long ts : timestampToValues.keySet()) {
            cells.add(toCell(rowKey, family, qualifier, ts, timestampToValues.get(ts)));
        }
        return cells;
    }

}
