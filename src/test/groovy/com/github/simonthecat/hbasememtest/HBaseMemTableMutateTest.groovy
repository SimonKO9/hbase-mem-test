package com.github.simonthecat.hbasememtest

import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.RowMutations
import org.apache.hadoop.hbase.filter.CompareFilter
import spock.lang.Specification

class HBaseMemTableMutateTest extends Specification {

    HBaseMemTable table;

    void setup() {
        table = HBaseMemTableBuilder.builder("test", "Animals")
                .put("cat", "general", "class", 1L, "mammal")
                .put("cat", "general", "legs", 1L, "4")
                .put("cat", "mammal", "body_temperature_celsius", 1L, "38.6")
                .build();
    }

    def "mutate row including put and delete should apply all mutations"() {
        def delete = new Delete("cat".bytes)
                .addColumn("general".bytes, "legs".bytes)

        def put = new Put("cat".bytes)
                .addColumn("general".bytes, "latin".bytes, "Felis catus".bytes)

        def mutations = new RowMutations("cat".bytes)
        mutations.add(delete)
        mutations.add(put)

        table.mutateRow(mutations)

        def result = table.get(new Get("cat".bytes))

        expect:
        result.getValue("general".bytes, "class".bytes) == "mammal".bytes
        result.getValue("general".bytes, "legs".bytes) == null
        result.getValue("general".bytes, "latin".bytes) == "Felis catus".bytes
        result.getValue("mammal".bytes, "body_temperature_celsius".bytes) == "38.6".bytes
    }

    def "check and mutate row including put and delete with matching condition should apply all mutations"() {
        def delete = new Delete("cat".bytes)
                .addColumn("general".bytes, "legs".bytes)

        def put = new Put("cat".bytes)
                .addColumn("general".bytes, "latin".bytes, "Felis catus".bytes)

        def mutations = new RowMutations("cat".bytes)
        mutations.add(delete)
        mutations.add(put)

        def mutateResult = table.checkAndMutate("cat".bytes, "general".bytes, "class".bytes, CompareFilter.CompareOp.EQUAL, "mammal".bytes, mutations)
        def result = table.get(new Get("cat".bytes))

        expect:
        mutateResult == true
        result.getValue("general".bytes, "class".bytes) == "mammal".bytes
        result.getValue("general".bytes, "legs".bytes) == null
        result.getValue("general".bytes, "latin".bytes) == "Felis catus".bytes
        result.getValue("mammal".bytes, "body_temperature_celsius".bytes) == "38.6".bytes
    }

    def "check and mutate row including put and delete with non-matching condition shouldn't change row values"() {
        def delete = new Delete("cat".bytes)
                .addColumn("general".bytes, "legs".bytes)

        def put = new Put("cat".bytes)
                .addColumn("general".bytes, "latin".bytes, "Felis catus".bytes)

        def mutations = new RowMutations("cat".bytes)
        mutations.add(delete)
        mutations.add(put)

        def mutateResult = table.checkAndMutate("cat".bytes, "general".bytes, "class".bytes, CompareFilter.CompareOp.EQUAL, "reptile".bytes, mutations)
        def result = table.get(new Get("cat".bytes))

        expect:
        mutateResult == false
        result.getValue("general".bytes, "class".bytes) == "mammal".bytes
        result.getValue("general".bytes, "legs".bytes) == "4".bytes
        result.getValue("general".bytes, "latin".bytes) == null
        result.getValue("mammal".bytes, "body_temperature_celsius".bytes) == "38.6".bytes
    }

}
