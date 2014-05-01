package com.hubspot.hbase.tasks.jobs;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import com.hubspot.hbase.tasks.compaction.SlowCompactionManager;
import com.hubspot.hbase.tasks.config.commandargs.ForArg;
import com.hubspot.hbase.tasks.config.commandargs.HBaseTaskOption;
import com.hubspot.hbase.tasks.hdfs.HdfsLocalityInfo;
import com.hubspot.hbase.tasks.load.RegionLoadEstimation;
import com.hubspot.hbase.tasks.models.RegionStats;
import org.apache.hadoop.hbase.HBaseAdminWrapper;
import org.apache.hadoop.hbase.ServerName;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class CompactionJob implements Runnable {

  public static final String SHORT_OPT = "compact";
  public static final String LONG_OPT = "compact";
  public static final String DESCRIPTION = "Requests compaction for every enabled table, or table specified (-table). Major compact is default, specify -compactionType minor to minor compact";

  private final HBaseAdminWrapper hBaseAdminWrapper;
  private final HdfsLocalityInfo hdfsLocalityInfo;
  private final RegionLoadEstimation regionLoadEstimation;
  private final SlowCompactionManager compactor;

  @Inject
  @ForArg(HBaseTaskOption.REVERSE_LOCALITY)
  Optional<Boolean> reverseLocality;

  @Inject
  @ForArg(HBaseTaskOption.TABLE)
  private Optional<String> tableToCompact;

  @Inject
  public CompactionJob(final HBaseAdminWrapper hBaseAdminWrapper,
                       final HdfsLocalityInfo hdfsLocalityInfo,
                       final RegionLoadEstimation regionLoadEstimation,
                       final SlowCompactionManager compactor) {
    this.hBaseAdminWrapper = hBaseAdminWrapper;
    this.hdfsLocalityInfo = hdfsLocalityInfo;
    this.regionLoadEstimation = regionLoadEstimation;
    this.compactor = compactor;
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
    final Multimap<ServerName, RegionStats> regions;
    if (tableToCompact.isPresent()) {
      regions = RegionStats.regionInfoToStats(hBaseAdminWrapper.getRegionInfosForTableByServer(tableToCompact.get()));
    } else {
      regions = RegionStats.regionInfoToStats(hBaseAdminWrapper.getRegionInfosByServer(true));
    }
    regionLoadEstimation.annotateRegionsWithLoadInPlace(regions);
    hdfsLocalityInfo.annotateRegionsWithLocalityInPlace(regions);
    List<RegionStats> regionList = Lists.newArrayList(regions.values());
    if (reverseLocality.or(false)) {
      regionList = orderByReverseLocality(regions);
    } else {
      regionList = Lists.newArrayList(regions.values());
      Collections.shuffle(regionList);
      Collections.sort(regionList, new Comparator<RegionStats>() {
        @Override
        public int compare(final RegionStats regionStats, final RegionStats regionStats2) {
          return Ints.compare(regionStats2.getHFileCount(), regionStats.getHFileCount());
        }
      });
    }
    compactor.compactAllRegions(regionList);
  }

  private List<RegionStats> orderByReverseLocality(final Multimap<ServerName, RegionStats> regions) throws Exception {
    List<RegionStats> regionsList = Lists.newArrayList(regions.values());

    Collections.sort(regionsList, new Comparator<RegionStats>() {
      @Override
      public int compare(final RegionStats o1, final RegionStats o2) {
        return ComparisonChain.start()
                .compare(o2.transferCost(o2.getServerName()), o1.transferCost(o1.getServerName()))
                .compare(o1.getModificationTime(), o2.getModificationTime())
                .result();
      }
    });
    return regionsList;

  }
}
