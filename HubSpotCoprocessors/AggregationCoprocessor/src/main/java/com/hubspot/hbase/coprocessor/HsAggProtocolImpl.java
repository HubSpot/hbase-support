package com.hubspot.hbase.coprocessor;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HsAggProtocolImpl extends BaseEndpointCoprocessor implements HsAggProtocol {
  @Override
  public Map<HashedBytes, Long> groupKeyCount(final Scan scan, final byte[] rowMask) throws IOException {
    final List<KeyValue> results = new ArrayList<KeyValue>();
    final Map<HashedBytes, Long> finalResult = new HashMap<HashedBytes, Long>();

    final InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
        .getRegion().getScanner(scan);
    try {
      boolean hasMoreRows;
      do {
        hasMoreRows = scanner.next(results);
        if (!results.isEmpty()) {
          final KeyValue kv = results.get(0);
          final byte[] masked = performMask(rowMask, kv.getRow());
          final HashedBytes mapKey = new HashedBytes(masked);
          final Long oldValue = finalResult.get(mapKey);
          if (oldValue == null) {
            finalResult.put(mapKey, 1L);
          } else {
            finalResult.put(mapKey, oldValue + 1L);
          }
        }
        results.clear();
      } while (hasMoreRows);
    } finally {
      scanner.close();
    }
    return finalResult;
  }

  private static final byte[] performMask(final byte[] mask, final byte[] input) {
    final byte[] result = Arrays.copyOf(input, Math.min(mask.length, input.length));
    for (int i = 0; i < result.length; ++i) {
      result[i] &= mask[i];
    }
    return result;
  }
}
