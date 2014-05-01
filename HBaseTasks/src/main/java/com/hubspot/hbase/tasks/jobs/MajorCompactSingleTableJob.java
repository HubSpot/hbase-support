package com.hubspot.hbase.tasks.jobs;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
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

import java.util.Map.Entry;

public class MajorCompactSingleTableJob implements Runnable {
  public static final String SHORT_OPT = "majorCompactTable";
  public static final String LONG_OPT = "majorCompactTable";
  public static final String DESCRIPTION = "Requests major compaction on a single table";

  @Inject
  @ForArg(HBaseTaskOption.TABLE)
  private Optional<String> tableToCompact;
  @Inject
  @ForArg(HBaseTaskOption.ALL_REGIONS)
  private Optional<Boolean> allRegions;

  private final HBaseAdminWrapper hBaseAdminWrapper;
  private final HdfsLocalityInfo hdfsLocalityInfo;
  private final RegionLoadEstimation regionLoadEstimation;
  private final SlowCompactionManager compactor;

  @Inject
  public MajorCompactSingleTableJob(final HBaseAdminWrapper hBaseAdminWrapper,
                                    final HdfsLocalityInfo hdfsLocalityInfo,
                                    final RegionLoadEstimation regionLoadEstimation,
                                    final SlowCompactionManager compactor) {
    this.hBaseAdminWrapper = hBaseAdminWrapper;
    this.regionLoadEstimation = regionLoadEstimation;
    this.hdfsLocalityInfo = hdfsLocalityInfo;
    this.compactor = compactor;
  }

  @Override
  public void run() {
    if (allRegions.or(false)) {
      try {
        compactAllRegions();
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    } else {
      try {
        hBaseAdminWrapper.get().majorCompact(tableToCompact.get());
      } catch (Throwable t) {
        throw Throwables.propagate(t);
      }
    }
  }

  private void compactAllRegions() throws Exception {

    Multimap<ServerName, HRegionInfo> regions = hBaseAdminWrapper.getRegionInfosByServer(true);
    Multimap<ServerName, RegionStats> regionList = RegionStats.regionInfoToStats(Multimaps.filterEntries(regions, new Predicate<Entry<ServerName, HRegionInfo>>() {
      @Override
      public boolean apply(Entry<ServerName, HRegionInfo> input) {
        return tableToCompact.get().equalsIgnoreCase(input.getValue().getTableNameAsString());
      }
    }));

    regionLoadEstimation.annotateRegionsWithLoadInPlace(regionList);
    hdfsLocalityInfo.annotateRegionsWithLocalityInPlace(regionList);

    compactor.compactAllRegions(regionList.values());

  }
}
