package com.hubspot.hbase.coprocessor;

import java.io.IOException;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

public interface CMSProtocol extends CoprocessorProtocol {
  void update(byte[] row, byte[] family, byte[] column, byte[] compressedCMS, long latestTimestamp) throws IOException;
}
