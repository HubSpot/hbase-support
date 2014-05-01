package com.hubspot.hbase.tasks.jobs;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.hubspot.hbase.tasks.hdfs.HBaseHdfsInfo;
import com.hubspot.hbase.tasks.hdfs.HdfsLocalityInfo;
import com.hubspot.hbase.tasks.models.RegionStats;
import com.hubspot.liveconfig.value.Value;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseAdminWrapper;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.HServerLoad.RegionLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ClusterStatusReporterJob implements Runnable {
  private static final Log LOG = LogFactory.getLog(ClusterStatusReporterJob.class);

  public static final String JOB_NAME = "clusterStatusReporter";
  public static final String DESCRIPTION = "Daemon which periodicially reports cluster status, including region-level stats, to OpenTSDB";

  private static final String METRIC_PREFIX = "hbase.regionserver.dynamic";
  private static final String SEPARATOR = ".";

  private final HBaseAdminWrapper hBaseAdminWrapper;
  private final HdfsLocalityInfo hdfsLocalityInfo;
  private final HBaseHdfsInfo hbaseHdfsInfo;
  //private final OpenTsdbClient tsdb;
  private final Value<Long> interval;
  private final Value<Long> localityInterval;
  private final Value<String> clusterName;

  @Inject
  public ClusterStatusReporterJob(HBaseAdminWrapper hbaseAdminWrapper,
                                  HdfsLocalityInfo hdfsLocalityInfo,
                                  HBaseHdfsInfo hbaseHdfsInfo,
                                  //OpenTsdbClient tsdb,
                                  @Named("hbase.clusterstatus.report.intervalMs") Value<Long> interval,
                                  @Named("hbase.clusterstatus.report.locality.intervalMs") Value<Long> localityInterval,
                                  @Named("hbase.clusterstatus.cluster.name") Value<String> clusterName) {
    this.hBaseAdminWrapper = hbaseAdminWrapper;
    this.hdfsLocalityInfo = hdfsLocalityInfo;
    this.hbaseHdfsInfo = hbaseHdfsInfo;
    //this.tsdb = tsdb;
    this.interval = interval;
    this.localityInterval = localityInterval;
    this.clusterName = clusterName;
  }


  @Override
  public void run() {
    ScheduledExecutorService localityExecutor = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("locality-thread-%s").build());
    ScheduledExecutorService clusterStatusExecutor = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("cluster-status-thread-%s").build());

    try {
      localityExecutor.scheduleAtFixedRate(new LocalityRunnable(), 0, localityInterval.get(), TimeUnit.MILLISECONDS);
      clusterStatusExecutor.scheduleAtFixedRate(new ClusterStatusRunnable(), 0, interval.get(), TimeUnit.MILLISECONDS);

      while (true) {
        try {
          Thread.sleep(60000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    } finally {
      System.out.println("Shutting down reporter threads.");
      localityExecutor.shutdownNow();
      clusterStatusExecutor.shutdownNow();
      System.out.println("Closing job resources.");
      try {
        hBaseAdminWrapper.get().close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      } finally {
        //tsdb.close();
      }
    }
  }

  private class ClusterStatusRunnable implements Runnable {

    @Override
    public void run() {
      try {
        HBaseAdmin admin = hBaseAdminWrapper.get();
        ClusterStatus status = admin.getClusterStatus();

        long timestamp = System.currentTimeMillis();
        for (ServerName server : status.getServers()) {

          HServerLoad serverLoad = status.getLoad(server);
          for (RegionLoad load : serverLoad.getRegionsLoad().values()) {
            Map<String, String> tags = Maps.newHashMap();

            String name = load.getNameAsString();

            String table = null;
            String region = null;

            if (name.contains(".META.")) {
              table = "META";
            } else if (name.contains("-ROOT-")) {
              table = "ROOT";
            } else {
              table = name.split(",")[0];
              String[] parts = name.split("\\.");
              region = parts[parts.length - 1];
            }

            putIfNotNull("table", table, tags);
            putIfNotNull("region", region, tags);

            Optional<String> optimizedLine = getReadOptimizedLinePart(table, region);

            // This is where we write our relevant statistics to tsdb:
            /*
            writeMetric(dataPointFactory,"stores", load.getStores(), timestamp, tags, optimizedLine);
            writeMetric(dataPointFactory,"storefiles", load.getStorefiles(), timestamp, tags, optimizedLine);
            writeMetric(dataPointFactory,"storefileSizeMB", load.getStorefileSizeMB(), timestamp, tags, optimizedLine);
            writeMetric(dataPointFactory,"memstoreSizeMB", load.getMemStoreSizeMB(), timestamp, tags, optimizedLine);
            writeMetric(dataPointFactory,"storefileIndexSizeMB", load.getStorefileIndexSizeMB(), timestamp, tags, optimizedLine);
            writeMetric(dataPointFactory,"readRequestsCount", load.getReadRequestsCount(), timestamp, tags, optimizedLine);
            writeMetric(dataPointFactory,"writeRequestsCount", load.getWriteRequestsCount(), timestamp, tags, optimizedLine);
            writeMetric(dataPointFactory,"rootIndexSizeKB", load.getRootIndexSizeKB(), timestamp, tags, optimizedLine);
            writeMetric(dataPointFactory,"totalStaticIndexSizeKB", load.getTotalStaticIndexSizeKB(), timestamp, tags, optimizedLine);
            writeMetric(dataPointFactory,"totalStaticBloomSizeKB", load.getTotalStaticBloomSizeKB(), timestamp, tags, optimizedLine);
            writeMetric(dataPointFactory,"totalCompactingKVs", load.getTotalCompactingKVs(), timestamp, tags, optimizedLine);
            writeMetric(dataPointFactory,"currentCompactedKVs", load.getCurrentCompactedKVs(), timestamp, tags, optimizedLine);
            */
          }
        }
      } catch (Throwable t) {
        LOG.fatal("Caught an error in ClusterStatusRunnable.  Exiting.", t);
        System.exit(1);
      }
    }

    private Optional<String> getReadOptimizedLinePart(String table, String region) {
      if (table != null && region != null) {
        return Optional.of(Joiner.on(SEPARATOR).join(table, region));
      }

      return Optional.absent();
    }

    private void putIfNotNull(String key, String value, Map<String, String> map) {
      if (value != null) {
        map.put(key, value);
      }
    }
  }

  private class LocalityRunnable implements Runnable {

    public void run() {
      try {
        Multimap<ServerName, RegionStats> regionInfos = RegionStats.regionInfoToStats(hBaseAdminWrapper.getRegionInfosByServer(true));
        hdfsLocalityInfo.annotateRegionsWithLocalityInPlace(regionInfos);
        hbaseHdfsInfo.annotateRegionsWithHdfsInfoInPlace(regionInfos);

        long timestamp = System.currentTimeMillis();
        for (ServerName server : regionInfos.keySet()) {
          Map<String, String> tags = Maps.newHashMap();
          for (RegionStats region : regionInfos.get(server)) {
            tags.put("table", region.getRegionInfo().getTableNameAsString());
            tags.put("region", region.getRegionInfo().getEncodedName());

            double localityPercent = region.localityFactor() * 100;

            //writeMetric(dataPointFactory, "localityIndex", localityPercent, timestamp, tags, Optional.<String> absent());
          }
        }
      } catch (Throwable t) {
        LOG.fatal("Caught an error in LocalityRunnable.  Exiting.", t);
        System.exit(1);
      }
    }
  }
}
