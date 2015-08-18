package com.github.simonthecat.hbasememtest;

import com.google.common.collect.Iterators;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ListBackedResultScanner implements ResultScanner {

    List<Result> results;
    Iterator<Result> iterator;

    public ListBackedResultScanner(List<Result> results) {
        this.results = results;
        this.iterator = results.iterator();
    }

    @Override
    public Result next() throws IOException {
        if (iterator.hasNext()) return iterator.next();
        else return null;
    }

    @Override
    public Result[] next(int nbRows) throws IOException {
        List<Result> results = new ArrayList<Result>(nbRows);

        int counter = nbRows;
        while (iterator.hasNext() && counter > 0) {
            results.add(iterator.next());
            counter--;
        }

        return results.toArray(new Result[results.size()]);
    }

    @Override
    public void close() {
        results = new ArrayList<Result>();
        iterator = Iterators.emptyIterator();
    }

    @Override
    public Iterator<Result> iterator() {
        return iterator;
    }
}
