package com.hubspot.hbase.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.io.OutputSupplier;

public class IO {

  private static Charset CHARSET = Charsets.UTF_8;
  
  public static void appendRegionsToFile(Map<HRegionInfo, ServerName> regions, String filename) throws IOException {
    writeRegionsToFile(regions, filename, true);
  }

  public static void writeRegionsToFile(Map<HRegionInfo, ServerName> regions, String filename) throws IOException {
    writeRegionsToFile(regions, filename, false);
  }

  private static void writeRegionsToFile(Map<HRegionInfo, ServerName> regions, String filename, boolean append) throws IOException {
    OutputStreamWriter writer = writerForFile(filename, append);

    for (Entry<HRegionInfo, ServerName> entry : regions.entrySet()) {
      writeRegionAndServer(entry.getKey(), entry.getValue(), writer);
    }

    writer.close();
  }
  
  public static OutputStreamWriter writerForFile(String filename, boolean append) throws IOException {
    OutputSupplier<OutputStreamWriter> os = Files.newWriterSupplier(new File(filename), CHARSET, append);
    return os.getOutput();
  }

  public static void filterRegionsByFile(Map<HRegionInfo, ServerName> regions, String filename) throws IOException {
    Map<String, HRegionInfo> regionsByEncodedName = getRegionsByEncodedName(regions.keySet());

    for (String region : getMappingsFromFile(filename).keySet()) {
      regions.remove(regionsByEncodedName.get(region));
    }
  }

  public static Map<HRegionInfo, ServerName> getRegionsExistingInFile(Set<HRegionInfo> allRegions, String filename) throws IOException {
    Map<String, HRegionInfo> regionsByEncodedName = getRegionsByEncodedName(allRegions);

    Map<HRegionInfo, ServerName> result = Maps.newHashMap();
    for (Entry<String, String> entry : getMappingsFromFile(filename).entrySet()) {
      result.put(regionsByEncodedName.get(entry.getKey()), new ServerName(entry.getValue()));
    }

    return result;
  }

  private static Map<String, HRegionInfo> getRegionsByEncodedName(Set<HRegionInfo> regions) {
    Map<String, HRegionInfo> regionsByEncodedName = Maps.newHashMapWithExpectedSize(regions.size());
    for (HRegionInfo region : regions) {
      regionsByEncodedName.put(region.getEncodedName(), region);
    }

    return regionsByEncodedName;
  }

  private static Map<String, String> getMappingsFromFile(String filename) throws IOException {
    final Map<String, String> map = Maps.newHashMap();
    BufferedReader reader = Files.newReader(new File(filename), CHARSET);
    String line;
    while ((line = reader.readLine()) != null) {
      String[] parts = line.trim().split("\\s");
      map.put(parts[0], parts[1]);
    }

    reader.close();

    return map;
  }

  public static void writeRegionAndServer(HRegionInfo region, ServerName serverName, OutputStreamWriter outputStreamWriter) throws IOException {
    outputStreamWriter.write(String.format("%s %s\n", region.getEncodedName(), serverName.toString()));
  }
}
