package com.hubspot.hbase.coprocessor;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTableInterface;

public class HBaseCMSs {

  private HBaseCMSs() {
  }

  public static void update(final HTableInterface table, final byte[] row, final byte[] family, final byte[] column, final MultiSetCounter counter) throws IOException {
    update(table, row, family, column, counter, System.currentTimeMillis());
  }

  public static void update(final HTableInterface table, final byte[] row, final byte[] family, final byte[] column, final MultiSetCounter counter, final long latestTimestamp) throws IOException {
    table.coprocessorProxy(CMSProtocol.class, row).update(row, family, column, counter.toBytes(), latestTimestamp);
  }
}
