package com.github.simonthecat.hbasememtest

import com.google.common.collect.Lists
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.Result
import spock.lang.Specification

class ListBackedResultScannerTest extends Specification {

    def "returns all results from list"() {
        def results = [
                Result.create(new KeyValue("row1".bytes, "f1".bytes, "q1".bytes, "v1".bytes)),
                Result.create(new KeyValue("row2".bytes, "f1".bytes, "q1".bytes, "v2".bytes)),
        ]

        expect:
        def scanner = new ListBackedResultScanner(results)
        Lists.newArrayList(scanner) == results
    }

    def "correctly returns batch results"() {
        def mockResults = [
                Result.create(new KeyValue("row1".bytes, "f1".bytes, "q1".bytes, "v1".bytes)),
                Result.create(new KeyValue("row2".bytes, "f1".bytes, "q1".bytes, "v2".bytes)),
                Result.create(new KeyValue("row3".bytes, "f1".bytes, "q1".bytes, "v3".bytes)),
                Result.create(new KeyValue("row4".bytes, "f1".bytes, "q1".bytes, "v4".bytes)),
        ]

        def scanner = new ListBackedResultScanner(mockResults)

        expect:
        scanner.next(2).toList() == mockResults.subList(0, 2)
    }

    def "correctly returns batch results if count is bigger than available results"() {
        def mockResults = [
                Result.create(new KeyValue("row1".bytes, "f1".bytes, "q1".bytes, "v1".bytes)),
                Result.create(new KeyValue("row2".bytes, "f1".bytes, "q1".bytes, "v2".bytes)),
        ]

        def scanner = new ListBackedResultScanner(mockResults)

        expect:
        scanner.next(4).toList() == mockResults
    }

    def "allows to navigate using both batch and single"() {
        def mockResults = [
                Result.create(new KeyValue("row1".bytes, "f1".bytes, "q1".bytes, "v1".bytes)),
                Result.create(new KeyValue("row2".bytes, "f1".bytes, "q1".bytes, "v2".bytes)),
                Result.create(new KeyValue("row3".bytes, "f1".bytes, "q1".bytes, "v3".bytes)),
                Result.create(new KeyValue("row4".bytes, "f1".bytes, "q1".bytes, "v4".bytes)),
        ]

        def scanner = new ListBackedResultScanner(mockResults)

        expect:
        [scanner.next()] + scanner.next(2).toList() + [scanner.next()] == mockResults
    }

    def "returns null after closing scanner"() {
        def mockResults = [
                Result.create(new KeyValue("row1".bytes, "f1".bytes, "q1".bytes, "v1".bytes)),
                Result.create(new KeyValue("row2".bytes, "f1".bytes, "q1".bytes, "v2".bytes)),
                Result.create(new KeyValue("row3".bytes, "f1".bytes, "q1".bytes, "v3".bytes)),
                Result.create(new KeyValue("row4".bytes, "f1".bytes, "q1".bytes, "v4".bytes)),
        ]

        def scanner = new ListBackedResultScanner(mockResults)

        expect:
        scanner.close()
        scanner.next() == null
    }
}
