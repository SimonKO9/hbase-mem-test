package com.github.simonthecat.hbasememtest

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.filter.RowFilter
import spock.lang.Specification
import spock.lang.Unroll

class HBaseMemTableScanTest extends Specification {

    HBaseMemTable table;

    void setup() {
        table = HBaseMemTableBuilder.builder("test", "Animals")
                .put("cat", "general", "class", 1L, "mammal")
                .put("cat", "general", "legs", 1L, "4")
                .put("cat", "mammal", "body_temperature_celsius", 1L, "38.6")
                .put("dog", "general", "class", 1L, "mammal")
                .put("dog", "general", "legs", 1L, "4")
                .put("dog", "mammal", "body_temperature_celsius", 1L, "38.3")
                .put("hawk", "general", "class", 1L, "bird")
                .put("hawk", "general", "legs", 1L, "2")
                .put("hawk", "bird", "wingspan_cm", 1L, "95")
                .build();
    }

    @Unroll
    def "default scan should return all rows"() {
        def scan = new Scan()
        def resultList = table.getScanner(scan).toList()

        expect:
        resultList.size() == 3
        resultList.get(id).getRow() == key.bytes
        resultList.get(id).getValue(family.bytes, qualifier.bytes) == value.bytes

        where:
        id | key    | family    | qualifier                  || value
        0  | "cat"  | "general" | "class"                    || "mammal"
        0  | "cat"  | "general" | "legs"                     || "4"
        0  | "cat"  | "mammal"  | "body_temperature_celsius" || "38.6"
        1  | "dog"  | "general" | "class"                    || "mammal"
        1  | "dog"  | "general" | "legs"                     || "4"
        1  | "dog"  | "mammal"  | "body_temperature_celsius" || "38.3"
        2  | "hawk" | "general" | "class"                    || "bird"
        2  | "hawk" | "general" | "legs"                     || "2"
        2  | "hawk" | "bird"    | "wingspan_cm"              || "95"
    }

    @Unroll
    def "scanning with families defined should include only those families"() {
        def scan = new Scan().addFamily("mammal".bytes)
        def resultList = table.getScanner(scan).toList()

        expect:
        resultList.size() == 2
        resultList.get(id).getRow() == key.bytes
        resultList.get(id).getValue(family.bytes, qualifier.bytes) == value

        where:
        id | key   | family    | qualifier                  || value
        0  | "cat" | "general" | "class"                    || null
        0  | "cat" | "mammal"  | "body_temperature_celsius" || "38.6".bytes
        1  | "dog" | "mammal"  | "body_temperature_celsius" || "38.3".bytes
    }

    @Unroll
    def "scanning with families and qualifiers defined should include only those qualifiers"() {
        def scan = new Scan().addColumn("general".bytes, "legs".bytes)
        def resultList = table.getScanner(scan).toList()

        expect:
        resultList.size() == 3
        resultList.get(id).getRow() == key.bytes
        resultList.get(id).getValue(family.bytes, qualifier.bytes) == value

        where:
        id | key    | family    | qualifier                  || value
        0  | "cat"  | "general" | "class"                    || null
        0  | "cat"  | "general" | "legs"                     || "4".bytes
        0  | "cat"  | "mammal"  | "body_temperature_celsius" || null
        1  | "dog"  | "general" | "legs"                     || "4".bytes
        2  | "hawk" | "general" | "legs"                     || "2".bytes
    }

    @Unroll
    def "scanning should respect filters"() {
        def filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new org.apache.hadoop.hbase.filter.BinaryComparator("hawk".bytes))
        def scan = new Scan().setFilter(filter)
        def resultList = table.getScanner(scan).toList()

        expect:
        resultList.size() == 1
        resultList.get(id).getRow() == key.bytes
        resultList.get(id).getValue(family.bytes, qualifier.bytes) == value.bytes

        where:
        id | key    | family    | qualifier     || value
        0  | "hawk" | "general" | "class"       || "bird"
        0  | "hawk" | "general" | "legs"        || "2"
        0  | "hawk" | "bird"    | "wingspan_cm" || "95"
    }

    def "scan families should create perform default scan with families defined"() {
        def resultList = table.getScanner("mammal".bytes).toList()

        expect:
        resultList.size() == 2
        resultList.get(id).getRow() == key.bytes
        resultList.get(id).getValue(family.bytes, qualifier.bytes) == value

        where:
        id | key   | family    | qualifier                  || value
        0  | "cat" | "general" | "class"                    || null
        0  | "cat" | "mammal"  | "body_temperature_celsius" || "38.6".bytes
        1  | "dog" | "mammal"  | "body_temperature_celsius" || "38.3".bytes
    }

    @Unroll
    def "scan families and qualifiers should perform default scan with qualifiers defined"() {
        def resultList = table.getScanner("general".bytes, "legs".bytes).toList()

        expect:
        resultList.size() == 3
        resultList.get(id).getRow() == key.bytes
        resultList.get(id).getValue(family.bytes, qualifier.bytes) == value

        where:
        id | key    | family    | qualifier                  || value
        0  | "cat"  | "general" | "class"                    || null
        0  | "cat"  | "general" | "legs"                     || "4".bytes
        0  | "cat"  | "mammal"  | "body_temperature_celsius" || null
        1  | "dog"  | "general" | "legs"                     || "4".bytes
        2  | "hawk" | "general" | "legs"                     || "2".bytes
    }
}
