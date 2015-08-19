package com.github.simonthecat.hbasememtest

import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Put
import spock.lang.Specification
import spock.lang.Unroll

class HBaseMemTablePutTest extends Specification {

    HBaseMemTable table;

    void setup() {
        table = HBaseMemTableBuilder.builder("test", "Animals")
                .put("cat", "general", "class", 1L, "mammal")
                .put("cat", "general", "legs", 1L, "4")
                .put("cat", "mammal", "body_temperature_celsius", 1L, "38.6")
                .build();
    }

    @Unroll
    def "put row should be available to get"() {
        def put = new Put("hawk".bytes)
                .addColumn("general".bytes, "class".bytes, "bird".bytes)
                .addColumn("general".bytes, "legs".bytes, "2".bytes)
                .addColumn("bird".bytes, "wingspan_cm".bytes, "95".bytes)

        table.put(put)

        def result = table.get(new Get("hawk".bytes))

        expect:
        result.getValue(family.bytes, qualifier.bytes) == value.bytes

        where:
        family    | qualifier     || value
        "general" | "class"       || "bird"
        "general" | "legs"        || "2"
        "bird"    | "wingspan_cm" || "95"
    }

    @Unroll
    def "putting existing value should create it's new version"() {
        def put = new Put("cat".bytes)
                .addColumn("general".bytes, "legs".bytes, "3".bytes)
        table.put(put)

        def result = table.get(new Get("cat".bytes).setMaxVersions(2))

        expect:
        def rowMap = result.getMap()
        def valueMap = rowMap.get(family.bytes).get(qualifier.bytes)
        valueMap.descendingKeySet().toList().collect { valueMap[it] } == values.collect { it.bytes }

        where:
        family    | qualifier                  || values
        "general" | "class"                    || ["mammal"]
        "general" | "legs"                     || ["4", "3"]
        "mammal"  | "body_temperature_celsius" || ["38.6"]
    }

    @Unroll
    def "multiple puts"() {
        def puts = [
                new Put("dog".bytes).addColumn("general".bytes, "class".bytes, "mammal".bytes),
                new Put("dog".bytes).addColumn("general".bytes, "legs".bytes, "4".bytes),
                new Put("dog".bytes).addColumn("mammal".bytes, "body_temperature_celsius".bytes, "38.3".bytes)
        ]
        table.put(puts)

        def result = table.get(new Get("dog".bytes))

        expect:
        result.getValue(family.bytes, qualifier.bytes) == value.bytes

        where:
        family    | qualifier                  || value
        "general" | "class"                    || "mammal"
        "general" | "legs"                     || "4"
        "mammal"  | "body_temperature_celsius" || "38.3"
    }
}
