package com.hubspot.hbase.tasks.jobs;

import com.google.common.base.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HMerge;

import com.google.inject.Inject;
import com.hubspot.hbase.tasks.config.commandargs.ForArg;
import com.hubspot.hbase.tasks.config.commandargs.HBaseTaskOption;

public class MergeTableRegionsJob implements Runnable {

  public static final String SHORT_OPT = "mergeRegionsInTable";
  public static final String LONG_OPT = "mergeRegionsInTable";
  public static final String DESCRIPTION = "DANGER! Merges regions in a disabled table.  Must major compact table prior to running.";
  
  @Inject @ForArg(HBaseTaskOption.TABLE)
  private Optional<String> tableName;
  @Inject @ForArg(HBaseTaskOption.ACTUALLY_RUN)
  private Optional<Boolean> actuallyRun;
  @Inject @ForArg(HBaseTaskOption.REGION_MERGE_FACTOR)
  private Optional<Integer> factor;
  @Inject @ForArg(HBaseTaskOption.EMPTY_REGIONS_ONLY)
  private Optional<Boolean> emptyRegionsOnly;

  private final Configuration configuration;

  private final FileSystem fs;
  
  @Inject
  public MergeTableRegionsJob(final FileSystem fs, Configuration configuration) {
    this.fs = fs;
    this.configuration = configuration;
  }
  
  @Override
  public void run() {
    if (!tableName.isPresent()) {
      throw new RuntimeException("Must pass the name of a disabled table to merge regions in.");
    }
    
    try {
      configuration.setInt(HMerge.REGION_FACTOR_CONF, factor.or(2));

      if (emptyRegionsOnly.or(false)) {
        HMerge.mergeEmptyRegions(configuration, fs, Bytes.toBytes(tableName.get()), actuallyRun.or(false));
      } else {
        HMerge.merge(configuration, fs, Bytes.toBytes(tableName.get()), actuallyRun.or(false));
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to merge regions in table " + tableName.get(), e);
    }
  }

}
