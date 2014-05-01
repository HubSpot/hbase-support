package com.hubspot.hbase.tasks.jobs;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.hubspot.hbase.tasks.compaction.SlowCompactionManager;
import com.hubspot.hbase.tasks.config.commandargs.ForArg;
import com.hubspot.hbase.tasks.config.commandargs.HBaseTaskOption;
import com.hubspot.hbase.tasks.hdfs.HdfsLocalityInfo;
import com.hubspot.hbase.tasks.load.RegionLoadEstimation;
import com.hubspot.hbase.tasks.models.RegionStats;
import org.apache.hadoop.hbase.HBaseAdminWrapper;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

import java.util.Set;

public class MajorCompactServersJob implements Runnable {
  public static final String SHORT_OPT = "majorCompactServers";
  public static final String LONG_OPT = "majorCompactServers";
  public static final String DESCRIPTION = "Major compact all regions on a server or list of servers";

  private final HBaseAdminWrapper wrapper;
  private final SlowCompactionManager compactor;
  private final RegionLoadEstimation regionLoadEstimation;
  private final HdfsLocalityInfo hdfsLocalityInfo;

  @Inject
  @ForArg(HBaseTaskOption.SERVER_NAME)
  private Optional<String> serverName;


  @Inject
  public MajorCompactServersJob(final HBaseAdminWrapper wrapper, final SlowCompactionManager compactor, final RegionLoadEstimation regionLoadEstimation, final HdfsLocalityInfo hdfsLocalityInfo) {
    this.wrapper = wrapper;
    this.compactor = compactor;
    this.regionLoadEstimation = regionLoadEstimation;
    this.hdfsLocalityInfo = hdfsLocalityInfo;
  }

  @Override
  public void run() {
    try {
      runJob();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private void runJob() throws Exception {
    final Set<String> servers = Sets.newHashSet();
    if (serverName.get().contains(",")) {
      for (String server : serverName.get().split(",")) {
        servers.add(server);
      }
    } else {
      servers.add(serverName.get());
    }

    Multimap<ServerName, HRegionInfo> regions = wrapper.getRegionInfosByServer(true);
    Multimap<ServerName, RegionStats> regionList = RegionStats.regionInfoToStats(Multimaps.filterKeys(regions, new Predicate<ServerName>() {
      @Override
      public boolean apply(ServerName input) {
        return servers.contains(input.getHostname());
      }

    }));
    regionLoadEstimation.annotateRegionsWithLoadInPlace(regionList);
    hdfsLocalityInfo.annotateRegionsWithLocalityInPlace(regionList);
    compactor.compactAllRegions(regionList.values());
  }

}
