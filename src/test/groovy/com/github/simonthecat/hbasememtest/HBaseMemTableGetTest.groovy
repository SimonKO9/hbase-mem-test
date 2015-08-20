package com.github.simonthecat.hbasememtest

import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator
import org.apache.hadoop.hbase.filter.ByteArrayComparable
import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.filter.ValueFilter
import spock.lang.Specification

class HBaseMemTableGetTest extends Specification {

    HBaseMemTable table;

    void setup() {
        table = HBaseMemTableBuilder.builder(TableName.valueOf("test", "Table"))
                .put("row1", "f1", "q1", 1L, "value11")
                .put("row1", "f1", "q1", 2L, "value11-updated")
                .put("row1", "f2", "q1", 1L, "value21")
                .put("row2", "f1", "q1", 1L, "value1")
                .put("row3", "f1", "q1", 1L, "value11")
                .put("row3", "f1", "q2", 1L, "value12")
                .put("row3", "f2", "q1", 1L, "value21")
                .build()
    }

    def "Get by row returns empty result for non-existent row"() {
        def get = new Get("nonexistent".bytes)

        def result = table.get(get)

        expect:
        result.isEmpty()
    }

    def "Get by row returns result containing most recent data for existing row"() {
        def get = new Get("row1".bytes)

        def result = table.get(get)

        expect:
        result.size() == 2
        result.getColumnCells("f1".bytes, "q1".bytes).get(0) == new KeyValue("row1".bytes, "f1".bytes, "q1".bytes, 2L, "value11-updated".bytes)
        result.getColumnCells("f2".bytes, "q1".bytes).get(0) == new KeyValue("row1".bytes, "f2".bytes, "q1".bytes, 1L, "value21".bytes)
    }

    def "Get by row returns result respecting max versions"() {
        def get = new Get("row1".bytes).setMaxVersions(2)

        def result = table.get(get)

        expect:
        result.size() == 3
        result.getColumnCells("f1".bytes, "q1".bytes).get(0) == new KeyValue("row1".bytes, "f1".bytes, "q1".bytes, 2L, "value11-updated".bytes)
        result.getColumnCells("f1".bytes, "q1".bytes).get(1) == new KeyValue("row1".bytes, "f1".bytes, "q1".bytes, 1L, "value11".bytes)
        result.getColumnCells("f2".bytes, "q1".bytes).get(0) == new KeyValue("row1".bytes, "f2".bytes, "q1".bytes, 1L, "value21".bytes)
    }

    def "Get by row returns result respecting family limits"() {
        def get = new Get("row3".bytes).addFamily("f2".bytes)

        def result = table.get(get)

        expect:
        result.size() == 1
        result.getColumnCells("f2".bytes, "q1".bytes).get(0) == new KeyValue("row3".bytes, "f2".bytes, "q1".bytes, 1L, "value12".bytes)
    }

    def "Get by row returns result respecting family and qualifier limits"() {
        def get = new Get("row3".bytes).addColumn("f1".bytes, "q2".bytes)

        def result = table.get(get)

        expect:
        result.size() == 1
        result.getColumnCells("f1".bytes, "q2".bytes).get(0) == new KeyValue("row3".bytes, "f1".bytes, "q2".bytes, 1L, "value12".bytes)
    }

    def "Get by row returns result respecting filters"() {
        ByteArrayComparable comparator = new BinaryPrefixComparator("value1".bytes)
        def filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, comparator);
        def get = new Get("row3".bytes).setFilter(filter)

        def result = table.get(get)

        expect:
        result.size() == 2
        result.getColumnCells("f1".bytes, "q1".bytes).get(0) == new KeyValue("row3".bytes, "f1".bytes, "q1".bytes, 1L, "value11".bytes)
        result.getColumnCells("f1".bytes, "q2".bytes).get(0) == new KeyValue("row3".bytes, "f1".bytes, "q2".bytes, 1L, "value12".bytes)
    }

    def "Multiple gets returns all results"() {
        def gets = [
                new Get("row2".bytes),
                new Get("row3".bytes).addColumn("f1".bytes, "q2".bytes)
        ]

        def result = table.get(gets)

        expect:
        result.collect { it.size() } == [1, 1]
        result[0].getColumnCells("f1".bytes, "q1".bytes).get(0) == new KeyValue("row2".bytes, "f1".bytes, "q1".bytes, 1L, "value1".bytes)
        result[1].getColumnCells("f1".bytes, "q2".bytes).get(0) == new KeyValue("row3".bytes, "f1".bytes, "q2".bytes, 1L, "value12".bytes)
    }

    def "Multiple gets includes empty results"() {
        def gets = [
                new Get("row2".bytes),
                new Get("nonexistent".bytes),
                new Get("row3".bytes).addColumn("f1".bytes, "q2".bytes)
        ]

        def result = table.get(gets)

        expect:
        result.collect { it.size() } == [1, 0, 1]
        result[0].getColumnCells("f1".bytes, "q1".bytes).get(0) == new KeyValue("row2".bytes, "f1".bytes, "q1".bytes, 1L, "value1".bytes)
        result[1].isEmpty()
        result[2].getColumnCells("f1".bytes, "q2".bytes).get(0) == new KeyValue("row3".bytes, "f1".bytes, "q2".bytes, 1L, "value12".bytes)
    }
}
