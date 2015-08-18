package com.github.simonthecat.hbasememtest;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;

import java.io.IOException;
import java.util.*;

import static org.apache.commons.lang.ArrayUtils.*;

public class HBaseMemTable implements Table {

    private TableName tableName;
    private InMemStore data;
    private CellConverter converter = new CellConverter();
    private BinaryComparator binaryComparator = new BinaryComparator();

    public HBaseMemTable(TableName tableName) {
        this.tableName = tableName;
        this.data = new InMemStore();
    }

    @Override
    public TableName getName() {
        return tableName;
    }

    @Override
    public Configuration getConfiguration() {
        return null;
    }

    @Override
    public HTableDescriptor getTableDescriptor() throws IOException {
        return new HTableDescriptor(tableName);
    }

    @Override
    public boolean exists(Get get) throws IOException {
        return data.containsKey(get.getRow());
    }

    @Override
    public boolean[] existsAll(List<Get> gets) throws IOException {
        if (gets == null || gets.isEmpty()) return new boolean[0];

        boolean[] results = new boolean[gets.size()];
        for (int i = 0; i < gets.size(); i++) {
            results[i] = exists(gets.get(i));
        }
        return results;
    }

    @Override
    public Result get(Get get) throws IOException {
        if (!exists(get)) return new Result();

        byte[] rowKey = get.getRow();
        if (!get.hasFamilies()) {
            return Result.create(converter.toCellList(rowKey, data.getByKey(rowKey)));
        }

        List<Cell> cells = new ArrayList<Cell>();

        for (byte[] family : get.familySet()) {
            NavigableSet<byte[]> qualifiers = get.getFamilyMap().get(family);
            if (qualifiers == null || qualifiers.isEmpty()) {
                qualifiers = data.getByKeyAndFamily(rowKey, family).navigableKeySet();
            }

            for (byte[] qualifier : qualifiers) {
                NavigableMap<Long, byte[]> timestampToValues = data.getByKeyAndFamilyAndQualifier(rowKey, family, qualifier);
                Long maxTimestamp = timestampToValues.lastKey();
                byte[] maxValue = timestampToValues.get(maxTimestamp);

                cells.add(converter.toCell(rowKey, family, qualifier, maxTimestamp, maxValue));
            }
        }

        return Result.create(cells);
    }

    @Override
    public Result[] get(List<Get> gets) throws IOException {
        Result[] results = new Result[gets.size()];

        for (int i = 0; i < gets.size(); i++) {
            results[i] = get(gets.get(i));
        }

        return results;
    }

    @Override
    public ResultScanner getScanner(Scan scan) throws IOException {
        List<Result> results = new ArrayList<Result>();

        for (byte[] rowKey : data.getRowKeys()) {
            // skip rows not in <start row, stop row> range
            if (!isEmpty(scan.getStartRow()) && binaryComparator.isGreater(scan.getStartRow(), rowKey)) {
                continue;
            }
            if (!isEmpty(scan.getStopRow()) && binaryComparator.isLessOrEqual(scan.getStopRow(), rowKey)) {
                continue;
            }


            // find matching cells (not filtered yet)
            List<Cell> cells;
            if (!scan.hasFamilies()) {
                cells = converter.toCellList(rowKey, data.getByKeyBetweenTimestamps(rowKey, scan.getTimeRange().getMin(), scan.getTimeRange().getMax()));
            } else {
                cells = new ArrayList<Cell>();
                for (byte[] family : scan.getFamilyMap().keySet()) {
                    NavigableSet<byte[]> qualifiers = scan.getFamilyMap().get(family);
                    if (qualifiers == null || qualifiers.isEmpty()) {
                        qualifiers = data.getByKeyAndFamily(rowKey, family).navigableKeySet();
                    }

                    for (byte[] qualifier : qualifiers) {
                        NavigableMap<Long, byte[]> timestampToValues = data.getByKeyAndFamilyAndQualifierBetweenTimestamps(rowKey, family, qualifier, scan.getTimeRange().getMin(), scan.getTimeRange().getMax());

                        cells.addAll(converter.toCellList(rowKey, family, qualifier, timestampToValues));
                    }
                }
            }

            // apply filters if defined
            List<Cell> filtered;
            if (scan.hasFilter()) {
                Filter filter = scan.getFilter();
                filtered = new ArrayList<Cell>(cells.size());

                filter.reset();

                for (Cell cell : cells) {
                    if (filter.filterAllRemaining()) break;
                    if (filter.filterRowKey(cell.getValueArray(), cell.getRowOffset(), cell.getRowLength())) {
                        continue;
                    }

                    Filter.ReturnCode filterResult = filter.filterKeyValue(cell);
                    if (filterResult == Filter.ReturnCode.INCLUDE) {
                        filtered.add(cell);
                    } else if (filterResult == Filter.ReturnCode.NEXT_ROW) {
                        break;
                    }
                }

                if (filter.hasFilterRow()) {
                    filter.filterRowCells(filtered);
                }
            } else {
                filtered = cells;
            }

            if (!filtered.isEmpty()) {
                results.add(Result.create(filtered));
            }
        }

        return new ListBackedResultScanner(results);
    }

    @Override
    public ResultScanner getScanner(byte[] family) throws IOException {
        Scan scan = new Scan();
        scan.addFamily(family);
        return getScanner(scan);
    }

    @Override
    public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
        Scan scan = new Scan();
        scan.addColumn(family, qualifier);
        return getScanner(scan);
    }

    @Override
    public void put(Put put) throws IOException {
        for (byte[] family : put.getFamilyCellMap().keySet()) {
            for (Cell cell : put.getFamilyCellMap().get(family)) {
                byte[] rowKey = CellUtil.cloneRow(cell);
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                long timestamp = System.currentTimeMillis();
                byte[] value = CellUtil.cloneValue(cell);
                data.insert(rowKey, family, qualifier, timestamp, value);
            }
        }
    }

    @Override
    public void put(List<Put> puts) throws IOException {
        for (Put put : puts) {
            put(put);
        }
    }

    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) throws IOException {
        NavigableMap<Long, byte[]> values = data.getByKeyAndFamilyAndQualifier(row, family, qualifier);
        if (values.isEmpty()) return false;

        if (Arrays.equals(values.lastEntry().getValue(), value)) {
            put(put);
            return true;
        }

        return false;
    }

    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, byte[] value, Put put) throws IOException {
        NavigableMap<Long, byte[]> values = data.getByKeyAndFamilyAndQualifier(row, family, qualifier);
        if (values.isEmpty()) return false;

        if (binaryComparator.byOperator(values.lastEntry().getValue(), value, compareOp)) {
            put(put);
            return true;
        }

        return false;
    }

    @Override
    public void delete(Delete delete) throws IOException {

    }

    @Override
    public void delete(List<Delete> deletes) throws IOException {

    }

    @Override
    public void batch(List<? extends Row> actions, Object[] results) throws IOException, InterruptedException {

    }

    @Override
    public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
        return new Object[0];
    }

    @Override
    public <R> void batchCallback(List<? extends Row> actions, Object[] results, Batch.Callback<R> callback) throws IOException, InterruptedException {

    }

    @Override
    public <R> Object[] batchCallback(List<? extends Row> actions, Batch.Callback<R> callback) throws IOException, InterruptedException {
        return new Object[0];
    }

    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete) throws IOException {
        return false;
    }

    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, byte[] value, Delete delete) throws IOException {
        return false;
    }

    @Override
    public void mutateRow(RowMutations rm) throws IOException {

    }

    @Override
    public Result append(Append append) throws IOException {
        return null;
    }

    @Override
    public Result increment(Increment increment) throws IOException {
        return null;
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount) throws IOException {
        return 0;
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, Durability durability) throws IOException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public CoprocessorRpcChannel coprocessorService(byte[] row) {
        return null;
    }

    @Override
    public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> service, byte[] startKey, byte[] endKey, Batch.Call<T, R> callable) throws ServiceException, Throwable {
        return null;
    }

    @Override
    public <T extends Service, R> void coprocessorService(Class<T> service, byte[] startKey, byte[] endKey, Batch.Call<T, R> callable, Batch.Callback<R> callback) throws ServiceException, Throwable {

    }

    @Override
    public long getWriteBufferSize() {
        return 0;
    }

    @Override
    public void setWriteBufferSize(long writeBufferSize) throws IOException {

    }

    @Override
    public <R extends Message> Map<byte[], R> batchCoprocessorService(Descriptors.MethodDescriptor methodDescriptor, Message request, byte[] startKey, byte[] endKey, R responsePrototype) throws ServiceException, Throwable {
        return null;
    }

    @Override
    public <R extends Message> void batchCoprocessorService(Descriptors.MethodDescriptor methodDescriptor, Message request, byte[] startKey, byte[] endKey, R responsePrototype, Batch.Callback<R> callback) throws ServiceException, Throwable {

    }

    @Override
    public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, byte[] value, RowMutations mutation) throws IOException {
        return false;
    }
}
