package com.hubspot.hbase.tasks.compaction;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.primitives.Doubles;
import com.google.common.util.concurrent.Atomics;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.hubspot.hbase.tasks.config.commandargs.ForArg;
import com.hubspot.hbase.tasks.config.commandargs.HBaseTaskOption;
import com.hubspot.hbase.tasks.helpers.jmx.RegionServerJMXInfo;
import com.hubspot.hbase.tasks.models.RegionStats;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseAdminWrapper;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Singleton
public class SlowCompactionManager {
  private final Log LOG = LogFactory.getLog(SlowCompactionManager.class);


  @Inject
  @ForArg(HBaseTaskOption.COMPACTION_QUIET_TIME)
  private Optional<Integer> compactionQuietTime;
  @Inject
  @ForArg(HBaseTaskOption.SIMULTANEOUS_COMPACTIONS)
  private Optional<Integer> simultaneousCompactions;
  @Inject
  @ForArg(HBaseTaskOption.MAX_TIME_MILLIS)
  private Optional<Long> maxCompactionTimeMillis;
  @Inject
  @ForArg(HBaseTaskOption.COMPACTION_TYPE)
  private Optional<String> compactionTypeOption;
  @Inject
  @ForArg(HBaseTaskOption.COMPACT_THRESHOLD)
  private Optional<Double> compactThreshold;

  private final HBaseAdminWrapper hBaseAdminWrapper;
  private final RegionServerJMXInfo regionServerJMXInfo;
  private final Map<ServerName, BlockingDeque<HRegionInfo>> serversToRegions;

  private final CountDownLatch started = new CountDownLatch(1);

  private volatile long regionServerUpdatedAt = 0L;
  private final AtomicReference<Map<HRegionInfo, ServerName>> regionServerInfo = Atomics.newReference();

  private AtomicInteger awaitingCompactions = new AtomicInteger(0);
  private AtomicInteger completedCompactions = new AtomicInteger(0);

  private List<Thread> compactionThreads;


  @Inject
  public SlowCompactionManager(final HBaseAdminWrapper hBaseAdminWrapper, final RegionServerJMXInfo regionServerJMXInfo) {
    this.hBaseAdminWrapper = hBaseAdminWrapper;
    this.regionServerJMXInfo = regionServerJMXInfo;
    this.serversToRegions = Maps.newConcurrentMap();
  }

  public synchronized void startCompactionQueue() throws Exception {
    if (started.getCount() < 1) return;


    this.compactionThreads = Lists.newArrayList();
    final Collection<ServerName> servers = hBaseAdminWrapper.get().getClusterStatus().getServers();
    for (final ServerName server : servers) {
      addServer(server);
    }
    started.countDown();
  }

  private synchronized void addServer(final ServerName serverName) {
    if (serversToRegions.containsKey(serverName)) return;
    serversToRegions.put(serverName, new LinkedBlockingDeque<HRegionInfo>());
    final Thread compactionThread = new Thread(new SingleServerCompaction(compactionQuietTime.get(), simultaneousCompactions.get(), serverName, serversToRegions.get(serverName)));
    compactionThread.setName(String.format("compaction-thread-%s", serverName.getHostname()));
    compactionThread.setDaemon(true);
    compactionThread.start();
    compactionThreads.add(compactionThread);
  }

  private synchronized Map<HRegionInfo, ServerName> getRegionLocations() throws IOException {
    if (regionServerInfo.get() == null || System.currentTimeMillis() - regionServerUpdatedAt > TimeUnit.MINUTES.toMillis(1)) {
      final Map<HRegionInfo, ServerName> newInfo = hBaseAdminWrapper.getAllRegionInfos(true);
      regionServerInfo.set(newInfo);
      regionServerUpdatedAt = System.currentTimeMillis();
    }
    return regionServerInfo.get();
  }

  public void compactRegionIfNecessary(final RegionStats stats) throws IOException {
    compactRegionOnServerIfNecessary(stats, getRegionLocations().get(stats.getRegionInfo()));
  }

  public void compactRegionOnServer(final HRegionInfo regionToCompact, final ServerName serverName) throws IOException {
    Preconditions.checkState(started.getCount() < 1, "Need to call startCompactionQueue() first.");
    addServer(serverName);
    serversToRegions.get(serverName).addLast(regionToCompact);
    awaitingCompactions.incrementAndGet();
  }

  public void compactRegionOnServerIfNecessary(final RegionStats stats, final ServerName serverName) throws IOException {
    if (stats.localityFactor(serverName) > compactThreshold.get()) {
      System.out.println(String.format(" Not compacting %s on %s because it's local.", stats.getRegionInfo(), serverName));
    } else {
      compactRegionOnServer(stats.getRegionInfo(), serverName);
    }
  }

  public void compactAllRegions(final Iterable<RegionStats> regionsToCompact) throws Exception {
    final Stopwatch stopwatch = new Stopwatch().start();
    startCompactionQueue();
    int i = 0;

    List<RegionStats> regions = Lists.newArrayList(regionsToCompact);
    Collections.sort(regions, new Comparator<RegionStats>() {
      @Override
      public int compare(final RegionStats o1, final RegionStats o2) {
        return Doubles.compare(o2.transferCost(o2.getServerName()), o1.transferCost(o1.getServerName()));
      }
    });

    for (final RegionStats region : regions) {
      compactRegionOnServerIfNecessary(region, region.getServerName());
      ++i;
    }
    System.out.println(String.format("Added %s regions to queue for compaction.", i));
    Thread.sleep(5000);
    if (maxCompactionTimeMillis.isPresent()) {
      awaitCompactions(maxCompactionTimeMillis.get() - stopwatch.stop().elapsedMillis(), TimeUnit.MILLISECONDS);
    } else {
      awaitCompactions();
    }
  }

  public void awaitCompactions() {
    awaitCompactions(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
  }

  public void awaitCompactions(long timeout, TimeUnit timeUnit) {
    final long totalNanos = timeUnit.toNanos(timeout);
    final long start = System.nanoTime();

    while (awaitingCompactions.get() > 0) {
      System.out.println(String.format(" Compacted %s regions. %s regions left in queue.", completedCompactions.get(), awaitingCompactions.get()));
      if (System.nanoTime() - start > totalNanos) {
        System.out.println(String.format(" Stopping job. We compacted %s regions", completedCompactions.get()));
        return;
      }
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  private class SingleServerCompaction implements Runnable {
    private static final int MAX_SINGLE_REGION_FAILURES = 5;
    private final int secondsBetweenCompactions;
    private final int maxCompactionQueuePerServer;
    private final ServerName server;
    private final BlockingDeque<HRegionInfo> regions;
    private final Multiset<HRegionInfo> regionFailures = HashMultiset.create();

    private SingleServerCompaction(final int secondsBetweenCompactions, final int maxCompactionQueuePerServer, final ServerName server, final BlockingDeque<HRegionInfo> regions) {
      this.secondsBetweenCompactions = secondsBetweenCompactions;
      this.maxCompactionQueuePerServer = maxCompactionQueuePerServer;
      this.server = server;
      this.regions = regions;
    }

    @Override
    public void run() {
      while (true) {
        final HRegionInfo region;
        try {
          region = regions.takeFirst();
        } catch (InterruptedException e) {
          awaitingCompactions.decrementAndGet();
          throw Throwables.propagate(e);
        }
        try {
          compactNextRegion(region);
          completedCompactions.incrementAndGet();
        } catch (Throwable t) {
          if (regionFailures.count(region) >= MAX_SINGLE_REGION_FAILURES) {
            LOG.error("Skipping region due to too many failures.", t);
            continue;
          }
          regionFailures.add(region);
          LOG.warn(t, t);
          regions.addLast(region);
        } finally {
          awaitingCompactions.decrementAndGet();
        }
      }
    }

    private void compactNextRegion(final HRegionInfo region) throws Throwable {
      waitUntilCompactionRoom();
      if (secondsBetweenCompactions > 0) {
        Thread.sleep(TimeUnit.SECONDS.toMillis(secondsBetweenCompactions));
      }
      waitUntilCompactionRoom();
      System.out.println(String.format("-- (%s) COMPACTING %s on %s", compactionTypeOption.get(), region.getRegionNameAsString(), server.getHostname()));
      boolean major = "MAJOR".equals(compactionTypeOption.get().toUpperCase());
      if (major) {
        hBaseAdminWrapper.get().majorCompact(region.getRegionName());
      } else {
        hBaseAdminWrapper.get().compact(region.getRegionName());
      }
      Thread.sleep(200);
    }

    private void waitUntilCompactionRoom() throws Exception {
      do {
        Thread.sleep(5000);
      } while (regionServerJMXInfo.getCompactionQueueSize(server.getHostname()) >= maxCompactionQueuePerServer);
    }
  }
}
