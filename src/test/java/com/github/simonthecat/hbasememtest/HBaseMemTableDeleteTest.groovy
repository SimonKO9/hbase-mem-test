package com.github.simonthecat.hbasememtest

import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.filter.CompareFilter
import spock.lang.Specification

class HBaseMemTableDeleteTest extends Specification {

    HBaseMemTable table;

    void setup() {
        table = HBaseMemTableBuilder.builder("test", "Animals")
                .put("cat", "general", "class", 10L, "reptile")
                .put("cat", "general", "class", 20L, "mammal")
                .put("cat", "general", "legs", 10L, "4")
                .put("cat", "mammal", "body_temperature_celsius", 20L, "38.6")
                .build();
    }

    def "delete non-existent row should have no effect"() {
        Delete delete = new Delete("non-existent row key".bytes)
        table.delete(delete);

        def result = table.get(new Get("cat".bytes))

        expect:
        result.getValue(family.bytes, qualifier.bytes) == value.bytes

        where:
        family    | qualifier                  || value
        "general" | "class"                    || "mammal"
        "general" | "legs"                     || "4"
        "mammal"  | "body_temperature_celsius" || "38.6"
    }

    def "delete existing row should delete all related cells"() {
        Delete delete = new Delete("cat".bytes)
        table.delete(delete);

        def result = table.get(new Get("cat".bytes))

        expect:
        result.isEmpty()
    }

    def "delete row's data by specifying family should delete only cells related to that family"() {
        Delete delete = new Delete("cat".bytes).addFamily("mammal".bytes)
        table.delete(delete);

        def result = table.get(new Get("cat".bytes))

        expect:
        result.getValue(family.bytes, qualifier.bytes) == value

        where:
        family    | qualifier                  || value
        "general" | "class"                    || "mammal".bytes
        "general" | "legs"                     || "4".bytes
        "mammal"  | "body_temperature_celsius" || null
    }

    def "delete row's data by specifying family and qualifier should delete only cells related to that qualifier"() {
        Delete delete = new Delete("cat".bytes).addColumn("general".bytes, "legs".bytes)
        table.delete(delete);

        def result = table.get(new Get("cat".bytes))

        expect:
        result.getValue(family.bytes, qualifier.bytes) == value

        where:
        family    | qualifier                  || value
        "general" | "class"                    || "mammal".bytes
        "general" | "legs"                     || null
        "mammal"  | "body_temperature_celsius" || "38.6".bytes
    }

    def "delete row's data should respect specified timestamp"() {
        Delete delete = new Delete("cat".bytes).addColumn("general".bytes, "class".bytes, 10L)
        table.delete(delete);

        def result = table.get(new Get("cat".bytes).setMaxVersions(2))

        expect:
        def rowMap = result.getMap()
        def valueMap = rowMap.get(family.bytes).get(qualifier.bytes)
        valueMap.descendingKeySet().toList().collect { valueMap[it] } == values.collect { it.bytes }

        where:
        family    | qualifier                  || values
        "general" | "class"                    || ["mammal"]
        "general" | "legs"                     || ["4"]
        "mammal"  | "body_temperature_celsius" || ["38.6"]
    }
}
