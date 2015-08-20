package com.github.simonthecat.hbasememtest

import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Get
import spock.lang.Specification

class HBaseMemTableCheckAndDeleteTest extends Specification {

    HBaseMemTable table;

    void setup() {
        table = HBaseMemTableBuilder.builder("test", "Animals")
                .put("cat", "general", "class", 1L, "mammal")
                .put("cat", "general", "legs", 1L, "2")
                .put("cat", "mammal", "body_temperature_celsius", 1L, "38.6")
                .build();
    }

    def "check and delete with non-matching condition should have no effect"() {
        def delete = new Delete("cat".bytes)
        def result = table.checkAndDelete("cat".bytes, "general".bytes, "class".bytes, "bird".bytes, delete)

        def getResult = table.get(new Get("cat".bytes))

        expect:
        result == false
        getResult.size() == 3
    }

    def "check and delete with matching condition should have effect"() {
        def delete = new Delete("cat".bytes)
        def result = table.checkAndDelete("cat".bytes, "general".bytes, "class".bytes, "mammal".bytes, delete)

        def getResult = table.get(new Get("cat".bytes))

        expect:
        result == true
        getResult.isEmpty()
    }
}
