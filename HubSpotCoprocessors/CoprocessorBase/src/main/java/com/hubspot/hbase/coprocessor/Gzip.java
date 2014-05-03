package com.hubspot.hbase.coprocessor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Gzip {
  public static byte[] compress(byte[] bytes) {
    try {
      byte[] gzippedResult = null;

      ByteArrayOutputStream byteOut = new ByteArrayOutputStream();

      OutputStream gzipOut = new GZIPOutputStream(byteOut);
      gzipOut.write(bytes);

      gzipOut.close();
      byteOut.close();

      gzippedResult = byteOut.toByteArray();

      return gzippedResult;
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  public static byte[] decompress(byte[] bytes) {
    try {
      GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(bytes));
      final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

      int nRead;
      byte[] data = new byte[2048];

      while ((nRead = in.read(data, 0, data.length)) != -1) {
        buffer.write(data, 0, nRead);
      }

      buffer.flush();

      return buffer.toByteArray();

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Gzip() {}
}
