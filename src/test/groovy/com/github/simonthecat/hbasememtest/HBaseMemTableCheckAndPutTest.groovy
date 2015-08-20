package com.github.simonthecat.hbasememtest

import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.filter.CompareFilter
import spock.lang.Specification

class HBaseMemTableCheckAndPutTest extends Specification {

    HBaseMemTable table;

    void setup() {
        table = HBaseMemTableBuilder.builder("test", "Animals")
                .put("cat", "general", "class", 1L, "mammal")
                .put("cat", "general", "legs", 1L, "2")
                .put("cat", "mammal", "body_temperature_celsius", 1L, "38.6")
                .build();
    }

    def "check and put with default operator and with matching value"() {
        def updateLegsPut = new Put("cat".bytes).addColumn("general".bytes, "legs".bytes, "4".bytes)
        def putResult = table.checkAndPut("cat".bytes, "general".bytes, "legs".bytes, "2".bytes, updateLegsPut);

        def result = table.get(new Get("cat".bytes))

        expect:
        putResult == true
        result.getValue(family.bytes, qualifier.bytes) == value.bytes

        where:
        family    | qualifier                  || value
        "general" | "class"                    || "mammal"
        "general" | "legs"                     || "4"
        "mammal"  | "body_temperature_celsius" || "38.6"
    }

    def "check and put with default operator and with non-matching value"() {
        def updateLegsPut = new Put("cat".bytes).addColumn("general".bytes, "legs".bytes, "4".bytes)
        def putResult = table.checkAndPut("cat".bytes, "general".bytes, "legs".bytes, "this-doesn't-match".bytes, updateLegsPut);

        def result = table.get(new Get("cat".bytes))

        expect:
        putResult == false
        result.getValue(family.bytes, qualifier.bytes) == value.bytes

        where:
        family    | qualifier                  || value
        "general" | "class"                    || "mammal"
        "general" | "legs"                     || "2"
        "mammal"  | "body_temperature_celsius" || "38.6"
    }

    def "check and put with default operator and with defined comparator"() {
        def updateLegsPut = new Put("cat".bytes).addColumn("general".bytes, "legs".bytes, "4".bytes)
        def putResult = table.checkAndPut("cat".bytes, "general".bytes, "class".bytes, CompareFilter.CompareOp.GREATER_OR_EQUAL, "mam".bytes, updateLegsPut);

        def result = table.get(new Get("cat".bytes))

        expect:
        putResult == true
        result.getValue(family.bytes, qualifier.bytes) == value.bytes

        where:
        family    | qualifier                  || value
        "general" | "class"                    || "mammal"
        "general" | "legs"                     || "4"
        "mammal"  | "body_temperature_celsius" || "38.6"
    }
}
