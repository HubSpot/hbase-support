package com.hubspot.hbase.coprocessor;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.util.HashedBytes;

import java.io.IOException;
import java.util.Map;

public interface HllProtocol extends CoprocessorProtocol {
  long addToHll(byte[] row, byte[] family, byte[] column, byte[] compressedHll, long latestTimestamp) throws IOException;
  KeyValue readHll(byte[] row, byte[] family, byte[] column) throws IOException;
  Map<HashedBytes, KeyValue> readHllBulk(Scan scan) throws IOException;
}
