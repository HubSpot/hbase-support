package com.hubspot.hbase.coprocessor;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;

public class CMSEndpoint extends BaseEndpointCoprocessor implements CMSProtocol {

  @Override
  public void update(byte[] row, byte[] family, byte[] column, byte[] compressedCMS, long latestTimestamp) throws IOException {
    final HRegion region = ((RegionCoprocessorEnvironment) getEnvironment()).getRegion();

    final Get get = new Get(row)
            .addColumn(family, column)
            .setMaxVersions(1);

    final byte[] newValue;
    long timestamp = latestTimestamp;

    Integer lid = region.getLock(null, row, true);
    try {
      final Result rowResult = region.get(get, lid);
      if (rowResult == null || rowResult.isEmpty()) {
        newValue = compressedCMS;
      } else {
        final KeyValue latestValue = rowResult.getColumnLatest(family, column);
        final MultiSetCounter oldCMS = MultiSetCounter.getCounter(latestValue.getValue());
        timestamp = Math.max(latestValue.getTimestamp(), latestTimestamp);
        oldCMS.mergeWith(MultiSetCounter.getCounter(compressedCMS));
        newValue = oldCMS.toBytes();
      }
      final Put put = new Put(row);
      put.add(family, column, timestamp, newValue);
      region.put(put, lid, true);
    } finally {
      region.releaseRowLock(lid);
    }
  }

}
