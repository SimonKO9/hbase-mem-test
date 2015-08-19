package com.github.simonthecat.hbasememtest;

import org.apache.hadoop.hbase.TableName;

public class HBaseMemTableBuilder {

    private InMemStore store = new InMemStore();
    private TableName tableName;

    private HBaseMemTableBuilder(TableName tableName) {
        this.tableName = tableName;
    }

    public static HBaseMemTableBuilder builder(TableName tableName) {
        return new HBaseMemTableBuilder(tableName);
    }

    public static HBaseMemTableBuilder builder(String namespace, String table) {
        return builder(TableName.valueOf(namespace, table));
    }

    public HBaseMemTableBuilder put(byte[] rowKey, byte[] family, byte[] qualifier, long timestamp, byte[] value) {
        store.insert(rowKey, family, qualifier, timestamp, value);
        return this;
    }

    public HBaseMemTableBuilder put(String rowKey, String family, String qualifier, long timestamp, String value) {
        return put(rowKey.getBytes(), family.getBytes(), qualifier.getBytes(), timestamp, value.getBytes());
    }

    public HBaseMemTable build() {
        return new HBaseMemTable(tableName, store);
    }

}
