package com.hubspot.hbase.coprocessor;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import java.io.IOException;
import java.util.Map;

public interface HsAggProtocol extends CoprocessorProtocol {
  Map<HashedBytes, Long> groupKeyCount(Scan scan, byte[] rowMask) throws IOException;
}
