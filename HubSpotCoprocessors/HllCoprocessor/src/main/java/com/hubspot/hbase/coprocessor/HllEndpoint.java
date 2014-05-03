package com.hubspot.hbase.coprocessor;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HashedBytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HllEndpoint extends BaseEndpointCoprocessor implements HllProtocol {

  public long addToHll(final byte[] row, final byte[] family, final byte[] column, final byte[] compressedHll, final long latestTimestamp) throws IOException {
    final HRegion region = ((RegionCoprocessorEnvironment)getEnvironment()).getRegion();

    final Get get = new Get(row);
    get.addColumn(family, column);
    get.setMaxVersions(1);
    long result = -1;
    final byte[] newValue;
    long timestamp = latestTimestamp;

    Integer lid = region.getLock(null, row, true);
    try {
      final Result rowResult = region.get(get, lid);
      if (rowResult == null || rowResult.isEmpty()) {
        newValue = compressedHll;
      } else {
        final KeyValue latestValue = rowResult.getColumnLatest(family, column);
        final HyperLogLog oldHll = HyperLogLog.Builder.build(Gzip.decompress(latestValue.getValue()));
        final long oldResult = oldHll.cardinality();
        timestamp = Math.max(latestValue.getTimestamp(), latestTimestamp);
        try {
          oldHll.addAll(HyperLogLog.Builder.build(Gzip.decompress(compressedHll)));
        } catch (CardinalityMergeException e) {
          throw new IOException(e);
        }
        result = oldHll.cardinality();
        if (result == oldResult) {
          return result;
        }
        result = oldResult;
        newValue = Gzip.compress(oldHll.getBytes());
      }
      final Put put = new Put(row);
      put.add(family, column, timestamp, newValue);
      region.put(put, lid, true);
    } finally {
      region.releaseRowLock(lid);
    }
    return result;
  }

  @Override
  public KeyValue readHll(final byte[] row, final byte[] family, final byte[] column) throws IOException {
    final HRegion region = ((RegionCoprocessorEnvironment)getEnvironment()).getRegion();
    final Get get = new Get(row);
    get.addColumn(family, column);
    get.setMaxVersions(1);
    Result rowResult = region.get(get, null);
    if (rowResult == null || rowResult.isEmpty()) {
      return new KeyValue(row, family, column, -1, Bytes.toBytes(-1L));
    }
    final KeyValue value = rowResult.raw()[0];
    return new KeyValue(row, family, column, value.getTimestamp(),
            Bytes.toBytes(SetCounter.getCounter(value.getValue()).cardinality()));
  }

  @Override
  public Map<HashedBytes, KeyValue> readHllBulk(final Scan scan) throws IOException {
    final List<KeyValue> results = new ArrayList<KeyValue>();
    final Map<HashedBytes, KeyValue> finalResult = new HashMap<HashedBytes, KeyValue>();

    final InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
            .getRegion().getScanner(scan);
    try {
      boolean hasMoreRows;
      do {
        hasMoreRows = scanner.next(results);
        for (KeyValue kv : results) {
          final KeyValue result = new KeyValue(kv.getRow(), kv.getFamily(), kv.getQualifier(), kv.getTimestamp(),
                  Bytes.toBytes(SetCounter.getCounter(kv.getValue()).cardinality()));
          finalResult.put(new HashedBytes(kv.getRow()), result);
        }
        results.clear();
      } while (hasMoreRows);
    } finally {
      scanner.close();
    }
    return finalResult;
  }
}
