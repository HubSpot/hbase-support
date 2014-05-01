package com.hubspot.hbase.tasks.hdfs;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.hubspot.hbase.tasks.config.HBaseTasksModule;
import com.hubspot.hbase.tasks.models.HdfsBlockInfo;
import com.hubspot.hbase.tasks.models.RegionHdfsInfo;
import com.hubspot.hbase.tasks.models.RegionStats;
import com.hubspot.hbase.utils.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;

import javax.inject.Named;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HdfsLocalityInfo {
  private final Path hbasePath;
  private final Map<String, ServerName> ipToServer;

  @Inject
  public HdfsLocalityInfo(@Named(HBaseTasksModule.HBASE_PATH) final Path hbasePath) {
    this.hbasePath = hbasePath;
    this.ipToServer = Maps.newHashMap();
  }

  public void annotateRegionsWithLocalityInPlace(final Multimap<ServerName, RegionStats> regionStats) throws Exception {
    loadServerIps(regionStats.keySet());
    final Map<Path, RegionStats> hdfsPathToRegion = computeHdfsPaths(regionStats.values());

    final String[] cmd = new String[]{"/bin/sh", "-c", String.format("hadoop fsck %s -files -blocks -locations", hbasePath.toString())};

    final Process p = Runtime.getRuntime().exec(cmd);
    try {
      final BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
      for (final Pair<String, List<HdfsBlockInfo>> entry : getIterable(ipToServer, reader)) {
        Path file = new Path(entry.getFirst());
        // Only include HFiles, which are 32 byte hex strings
        if (!file.getName().matches("[a-f0-9]{32}")) {
          continue;
        }

        while (!hbasePath.equals(file) && file != null) {
          final RegionStats region = hdfsPathToRegion.get(file);
          if (region == null) {
            file = file.getParent();
            // Exclude internal directories, which start with a period
            if (file.getName().startsWith(".")) {
              break;
            }
          } else {
            region.addServerStorage(new RegionHdfsInfo(entry.getSecond()));
            break;
          }
        }
      }
    } finally {
      handleError(cmd, p);
      p.destroy();
    }
  }

  private void handleError(final String[] cmd, final Process p) {
    try {
      if (p.exitValue() != 0) {
        System.err.println("Error running cmd: " + Arrays.asList(cmd));
        for (int c = p.getErrorStream().read(); c != -1; c = p.getErrorStream().read()) {
          System.err.print((char)c);
        }
      }
    } catch (Throwable t) {}
  }

  private Map<Path, RegionStats> computeHdfsPaths(final Collection<RegionStats> regions) {
    final Map<Path, RegionStats> result = Maps.newHashMap();
    for (final RegionStats region : regions) {
      final Path regionPath = new Path(HTableDescriptor.getTableDir(hbasePath, region.getRegionInfo().getTableName()), region.getRegionInfo().getEncodedName());
      result.put(regionPath, region);
    }
    return result;
  }

  private void loadServerIps(final Collection<ServerName> servers) throws UnknownHostException {
    for (final ServerName server : servers) {
      final InetAddress inetAddress = InetAddress.getByName(server.getHostname());
      ipToServer.put(inetAddress.getHostAddress(), server);
    }
  }

  private Iterable<Pair<String, List<HdfsBlockInfo>>> getIterable(final Map<String, ServerName> ipToServer, final BufferedReader reader) {
    final Pattern FILE_PATTERN = Pattern.compile("^(\\S+) (\\d+) bytes, (\\d+) block.*");
    final Pattern BLOCK_PATTERN = Pattern.compile("^\\d+.*? len=(\\d+).*?\\[(.*?)\\].*");
    final Iterator<Pair<String, List<HdfsBlockInfo>>> iterator = new AbstractIterator<Pair<String, List<HdfsBlockInfo>>>() {
      @Override
      protected Pair<String, List<HdfsBlockInfo>> computeNext() {
        String line;
        String currentFile = null;
        final List<HdfsBlockInfo> currentBlocks = Lists.newArrayList();

        while ((line = getLine()) != null) {
          if (line.trim().isEmpty()) {
            if (currentFile != null && !currentBlocks.isEmpty()) {
              break;
            } else {
              continue;
            }
          }

          if (currentFile == null) {
            final Matcher fileMatcher = FILE_PATTERN.matcher(line);
            if (fileMatcher.matches()) {
              currentFile = fileMatcher.group(1);
            }
          } else {
            final Matcher blockMatcher = BLOCK_PATTERN.matcher(line);
            if (blockMatcher.matches()) {
              final long length = Long.valueOf(blockMatcher.group(1));
              final Set<ServerName> servers = Sets.newHashSet();
              for (final String serverPiece : Splitter.on(", ").omitEmptyStrings().split(blockMatcher.group(2))) {
                final String ip = serverPiece.split(":")[0];
                if (ipToServer.containsKey(ip)) {
                  servers.add(ipToServer.get(ip));
                }
              }
              currentBlocks.add(new HdfsBlockInfo(length, servers));
            }
          }
        }
        if (!currentBlocks.isEmpty()) {
          return Pair.of(currentFile, currentBlocks);
        }
        return endOfData();
      }

      private String getLine() {
        try {
          return reader.readLine();
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };

    return new Iterable<Pair<String, List<HdfsBlockInfo>>>() {
      @Override
      public Iterator<Pair<String, List<HdfsBlockInfo>>> iterator() {
        return iterator;
      }
    };
  }
}
