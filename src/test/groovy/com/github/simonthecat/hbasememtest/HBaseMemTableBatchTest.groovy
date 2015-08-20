package com.github.simonthecat.hbasememtest

import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Row
import spock.lang.Specification

class HBaseMemTableBatchTest extends Specification {

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

    def "batch does nothing when feeded with empty action list"() {
        List<? extends Row> actions = []

        def results = table.batch(actions)

        expect:
        results.length == 0
    }

    def "batch executes all provided actions and returns results with same size"() {
        List<? extends Row> actions = [
                new Put("cat".bytes).addColumn("general".bytes, "has_tail".bytes, "true".bytes),
                new Delete("cat".bytes).addColumn("general".bytes, "legs".bytes),
                new Get("hawk".bytes).addColumn("general".bytes, "class".bytes)
        ]

        def results = table.batch(actions)

        expect:
        results[2] instanceof Result
    }

    def "batch executes all provided actions and fills provided array with results"() {
        List<? extends Row> actions = [
                new Put("cat".bytes).addColumn("general".bytes, "has_tail".bytes, "true".bytes),
                new Delete("cat".bytes).addColumn("general".bytes, "legs".bytes),
                new Get("hawk".bytes).addColumn("general".bytes, "class".bytes)
        ]

        def results = new Object[actions.size()];
        table.batch(actions, results)

        expect:
        results[2] instanceof Result
    }
}
