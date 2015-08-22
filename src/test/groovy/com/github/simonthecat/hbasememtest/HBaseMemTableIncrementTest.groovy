package com.github.simonthecat.hbasememtest

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Increment
import org.apache.hadoop.hbase.util.Bytes
import spock.lang.Specification

class HBaseMemTableIncrementTest extends Specification {

    HBaseMemTable table;

    void setup() {
        table = HBaseMemTableBuilder.builder(TableName.valueOf("test", "Table"))
                .put("row1".bytes, "f1".bytes, "q1".bytes, 1L, Bytes.toBytes(10L))
                .put("row2".bytes, "f1".bytes, "q1".bytes, 1L, Bytes.toBytes(100L))
                .put("row2".bytes, "f1".bytes, "q2".bytes, 1L, Bytes.toBytes(200L))
                .put("row2".bytes, "f1".bytes, "q3".bytes, 1L, Bytes.toBytes(300L))
                .build()
    }

    def "incrementing existing value should update it's value"() {
        def incrementResult = table.incrementColumnValue("row1".bytes, "f1".bytes, "q1".bytes, 5L)
        def getResult = table.get(new Get("row1".bytes).addColumn("f1".bytes, "q1".bytes))

        expect:
        incrementResult == 15L
        getResult.getValue("f1".bytes, "q1".bytes) == Bytes.toBytes(15L)
    }

    def "incrementing non-existent value should create it and initialize to amount"() {
        def incrementResult = table.incrementColumnValue("row3".bytes, "f1".bytes, "q1".bytes, 9L)
        def getResult = table.get(new Get("row3".bytes).addColumn("f1".bytes, "q1".bytes))

        expect:
        incrementResult == 9L
        getResult.getValue("f1".bytes, "q1".bytes) == Bytes.toBytes(9L)
    }

    def "increment multiple columns should update all values and include them in result"() {
        def increment = new Increment("row2".bytes)
                .addColumn("f1".bytes, "q1".bytes, 1L)
                .addColumn("f1".bytes, "q2".bytes, 2L)
                .addColumn("f1".bytes, "q3".bytes, 3L)

        def incrementResult = table.increment(increment)

        expect:
        incrementResult.size() == 3
        incrementResult.getValue("f1".bytes, "q1".bytes) == Bytes.toBytes(101L)
        incrementResult.getValue("f1".bytes, "q2".bytes) == Bytes.toBytes(202L)
        incrementResult.getValue("f1".bytes, "q3".bytes) == Bytes.toBytes(303L)
    }

}
