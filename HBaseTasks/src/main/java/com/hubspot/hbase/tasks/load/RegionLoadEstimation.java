package com.hubspot.hbase.tasks.load;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import com.google.common.base.Throwables;
import com.hubspot.hbase.tasks.hdfs.HBaseHdfsInfo;
import com.hubspot.hbase.tasks.helpers.jmx.RegionServerJMXInfo;
import com.hubspot.hbase.tasks.models.RegionStats;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


public class RegionLoadEstimation {
  private final HBaseHdfsInfo hBaseHdfsInfo;
  private final RegionServerJMXInfo regionServerJMXInfo;
  private static final ExecutorService jmxExecutorService = Executors.newFixedThreadPool(100);

  @Inject
  public RegionLoadEstimation(final HBaseHdfsInfo hBaseHdfsInfo, final RegionServerJMXInfo regionServerJMXInfo) {
    this.hBaseHdfsInfo = hBaseHdfsInfo;
    this.regionServerJMXInfo = regionServerJMXInfo;
  }

  public List<RegionStats> getLoadInformationForRegions(final ServerName serverName, final List<HRegionInfo> hRegionInfos) {
    final ArrayListMultimap<ServerName, RegionStats> regionStats = ArrayListMultimap.create(1, hRegionInfos.size());
    for (final HRegionInfo regionInfo : hRegionInfos) {
      regionStats.put(serverName, new RegionStats(regionInfo, serverName));
    }
    annotateRegionsWithLoadInPlace(regionStats);
    return regionStats.get(serverName);
  }

  public void annotateRegionsWithLoadInPlace(final Multimap<ServerName, RegionStats> regionStats) {
    try {
      ensureSize(regionStats.values());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }

    if (regionStats.isEmpty()) {
      return;
    } else if (regionStats.keySet().size() == 1) {
      final ServerName server = regionStats.keySet().iterator().next();
      annotateSingleServer(server, regionStats.get(server));
      return;
    }

    final ExecutorService executorService = Executors.newFixedThreadPool(regionStats.keySet().size());
    final List<Future<?>> futures = Lists.newLinkedList();

    for (final ServerName server : regionStats.keySet()) {
      futures.add(executorService.submit(new Runnable() {
        @Override
        public void run() {
          annotateSingleServer(server, regionStats.get(server));
        }
      }));
    }

    try {
      for (final Future<?> future : futures) future.get();
      executorService.shutdown();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private void annotateSingleServer(final ServerName server, final Iterable<RegionStats> regionStats) {
    final double totalLatency = regionServerJMXInfo.get75thPercentileReadLatency(server.getHostname());
    long totalSize = 0L;
    for (final RegionStats region : regionStats) {
      if (!region.hasSize()) {
        region.updateWith(hBaseHdfsInfo.getRegionStats(region.getRegionInfo(), region.getServerName()));
      }
      totalSize += region.getSize();
    }

    for (final RegionStats region : regionStats) {
      if (!region.hasSize() || region.getSize() == 0L || totalSize == 0L) {
        region.setLoadWeight(0d);
      } else {
        region.setLoadWeight((totalLatency / totalSize) * region.getSize());
      }
    }
  }

  private void ensureSize(final Iterable<RegionStats> regionStats) throws ExecutionException, InterruptedException {
    final List<Future<?>> futures = Lists.newLinkedList();
    for (final RegionStats region : regionStats) {
      futures.add(jmxExecutorService.submit(new Runnable() {
        @Override
        public void run() {
          region.updateWith(hBaseHdfsInfo.getRegionStats(region.getRegionInfo(), region.getServerName()));
        }
      }));
    }
    for (final Future future : futures) {
      future.get();
    }
  }
}
