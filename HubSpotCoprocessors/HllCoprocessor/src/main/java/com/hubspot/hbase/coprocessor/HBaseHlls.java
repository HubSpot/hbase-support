package com.hubspot.hbase.coprocessor;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HashedBytes;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HBaseHlls {
  private HBaseHlls() {}

  public static <T> long updateSetCounter(final HTableInterface table, final byte[] row, final byte[] family, final byte[] column, final SetCounter<T> counter) throws IOException {
    return updateSetCounter(table, row, family, column, counter, System.currentTimeMillis());
  }

  public static <T> long updateSetCounter(final HTableInterface table, final byte[] row, final byte[] family, final byte[] column, final SetCounter<T> counter, final long latestTimestamp) throws IOException {
    return table.coprocessorProxy(HllProtocol.class, row).addToHll(row, family, column, counter.toBytes(), latestTimestamp);
  }

  public static HllResult getCardinality(final HTableInterface table, final byte[] row, final byte[] family, final byte[] column) throws IOException {
    final KeyValue result = table.coprocessorProxy(HllProtocol.class, row).readHll(row, family, column);
    return new HllResult(Bytes.toLong(result.getValue()), result.getTimestamp());
  }

  public static Map<HashedBytes, HllResult> getCardinalityBulk(final HTableInterface table, final Scan scan) throws Throwable {
    final Map<HashedBytes, HllResult> result = new ConcurrentHashMap<HashedBytes, HllResult>();
    table.coprocessorExec(HllProtocol.class, scan.getStartRow(), scan.getStopRow(), new Batch.Call<HllProtocol, Map<HashedBytes, KeyValue>>() {
              @Override
              public Map<HashedBytes, KeyValue> call(final HllProtocol instance) throws IOException {
                return instance.readHllBulk(scan);
              }
            }, new Batch.Callback<Map<HashedBytes, KeyValue>>() {
              @Override
              public void update(final byte[] region, final byte[] row, final Map<HashedBytes, KeyValue> regionResult) {
                for (Map.Entry<HashedBytes, KeyValue> entry : regionResult.entrySet()) {
                  result.put(entry.getKey(), new HllResult(Bytes.toLong(entry.getValue().getValue()), entry.getValue().getTimestamp()));
                }
              }
            }
    );
    return result;
  }
}
