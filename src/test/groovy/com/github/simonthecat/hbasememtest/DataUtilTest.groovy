package com.github.simonthecat.hbasememtest

import spock.lang.Specification

class DataUtilTest extends Specification {

    def "create NavigableMap from empty map"() {
        Map<byte[], Map<byte[], Map<byte[], Map<Long, byte[]>>>> map = [:]

        expect:
        DataUtil.fromMap(map).isEmpty()
    }

    def "create NavigableMap from non-empty map"() {
        Map<byte[], Map<byte[], Map<byte[], Map<Long, byte[]>>>> map = [
                ("row1".bytes): [
                        ("f1".bytes): [
                                ("q1".bytes): [
                                        1L: "ok".bytes,
                                        2L: "ok".bytes
                                ]
                        ]
                ]
        ]

        expect:
        DataUtil.fromMap(map) == map
    }

    def "create NavigableMap from non-empty string map"() {
        Map<String, Map<String, Map<String, Map<Long, String>>>> map = [
                "row1": [
                        "f1": [
                                "q1": [
                                        1L: "1",
                                        2L: "2"
                                ]
                        ]
                ]
        ]

        expect:
        def result = DataUtil.fromStringMap(map)
        result["row1".bytes]["f1".bytes]["q1".bytes][1L] == "1".bytes
        result["row1".bytes]["f1".bytes]["q1".bytes][2L] == "2".bytes
    }

}
