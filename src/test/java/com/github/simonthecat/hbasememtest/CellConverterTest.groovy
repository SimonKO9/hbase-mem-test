package com.github.simonthecat.hbasememtest

import org.apache.hadoop.hbase.KeyValue
import spock.lang.Specification

class CellConverterTest extends Specification {

    def "correctly converts to single cell"() {
        def converter = new CellConverter()

        expect:
        converter.toCell("k1".bytes, "f1".bytes, "q".bytes, 1L, "v1".bytes) == new KeyValue("k1".bytes, "f1".bytes, "q".bytes, 1L, "v1".bytes)
    }

    def "correctly converts single row's data to cell list"() {
        def converter = new CellConverter()
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> data = DataUtil.fromStringMap([
                "k1": [
                        "f1": [
                                "q1": [
                                        1L: "a",
                                        2L: "b"
                                ],
                                "q2": [
                                        1L: "c"
                                ]
                        ]
                ]
        ])

        expect:
        converter.toCellList("k1".bytes, data.get("k1".bytes), get.getMaxVersions()) == [
                new KeyValue("k1".bytes, "f1".bytes, "q1".bytes, 1L, "a".bytes),
                new KeyValue("k1".bytes, "f1".bytes, "q1".bytes, 2L, "b".bytes),
                new KeyValue("k1".bytes, "f1".bytes, "q2".bytes, 1L, "c".bytes)
        ]
    }

    def "correctly converts to single row's data with multiple versions"() {
        def converter = new CellConverter()
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> data = DataUtil.fromStringMap([
                "k1": [
                        "f1": [
                                "q1": [
                                        1L: "a",
                                        2L: "b"
                                ]
                        ]
                ]
        ])

        expect:
        converter.toCellList("k1".bytes, "f1".bytes, "q1".bytes, data.get("k1".bytes).get("f1".bytes).get("q1".bytes)) == [
                new KeyValue("k1".bytes, "f1".bytes, "q1".bytes, 1L, "a".bytes),
                new KeyValue("k1".bytes, "f1".bytes, "q1".bytes, 2L, "b".bytes),
        ]
    }

}
