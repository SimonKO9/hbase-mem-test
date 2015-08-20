package com.github.simonthecat.hbasememtest

import spock.lang.Specification
import spock.lang.Unroll

@Unroll
class BinaryComparatorTest extends Specification {

    def "compare #left and #right is #cmp"() {
        expect:
        new BinaryComparator().compare(left.bytes, right.bytes) == expected

        where:
        left  | right || expected
        "a"   | "a"   || 0
        "a"   | "b"   || -1
        "b"   | "a"   || 1
        "bbb" | "a"   || 1
        "b"   | "aaa" || 1
        "aaa" | "b"   || -1
        "a"   | "bbb" || -1
        ""    | "a"   || -1
        "a"   | ""    || 1
    }

}
