package com.hubspot.hbase.coprocessor;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

public class HubSpotAggregations {
  /**
   * Count the keys by the mask.
   *
   * Suppose you have a bunch of keys that are of the form
   *
   *   YYYY-MM-DD
   *
   * And you wanted to count the rows by year.
   *
   * Simply call:
   * HubSpotAggregations.groupKeyCount(table, scan, new byte[]{0xFF, 0xFF, 0xFF, 0xFF});
   *
   * @param table - The table to query
   * @param scan - The scan to use with the proper start and stop keys.
   * @param rowMask - The bitmask by which to filter the row.
   * @return Map of row key to count.
   * @throws Throwable
   */
  public static Map<HashedBytes, Long> groupKeyCount(final HTableInterface table, final Scan scan, final byte[] rowMask) throws Throwable {
    final ConcurrentLinkedQueue<Map<HashedBytes, Long>> results = new ConcurrentLinkedQueue<Map<HashedBytes, Long>>();

    table.coprocessorExec(HsAggProtocol.class, scan.getStartRow(), scan.getStopRow(), new Batch.Call<HsAggProtocol, Map<HashedBytes, Long>>() {
              @Override
              public Map<HashedBytes, Long> call(final HsAggProtocol instance) throws IOException {
                return instance.groupKeyCount(scan, rowMask);
              }
            }, new Batch.Callback<Map<HashedBytes, Long>>() {
              @Override
              public void update(final byte[] region, final byte[] row, final Map<HashedBytes, Long> regionResult) {
                results.add(regionResult);
              }});

    final Map<HashedBytes, Long> result = new HashMap<HashedBytes, Long>();

    for (final Map<HashedBytes, Long> regionResult : results) {
      for (final Map.Entry<HashedBytes, Long> entry : regionResult.entrySet()) {
        final HashedBytes key = entry.getKey();
        final Long value = entry.getValue();
        final Long oldValue = result.get(key);
        if (oldValue == null) {
          result.put(key, value);
        } else {
          result.put(key, value + oldValue);
        }
      }
    }
    return result;
  }

  private HubSpotAggregations() {}
}
