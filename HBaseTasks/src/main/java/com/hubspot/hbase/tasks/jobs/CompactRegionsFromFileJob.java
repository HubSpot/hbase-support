package com.hubspot.hbase.tasks.jobs;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.hubspot.hbase.tasks.config.commandargs.ForArg;
import com.hubspot.hbase.tasks.config.commandargs.HBaseTaskOption;
import com.hubspot.hbase.utils.IO;
import org.apache.hadoop.hbase.HBaseAdminWrapper;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

import java.util.Map;
import java.util.Map.Entry;

public class CompactRegionsFromFileJob implements Runnable {
  public static final String SHORT_OPT = "compactRegionsFromFile";
  public static final String LONG_OPT = "compactRegionsFromFile";
  public static final String DESCRIPTION = "Compact regions specified in a file, if inputFile is specified.  If outputFile is specified, dump all regions to a file.";

  @Inject
  @ForArg(HBaseTaskOption.INPUT_FILE)
  private Optional<String> inputFile;
  @Inject
  @ForArg(HBaseTaskOption.OUTPUT_FILE)
  private Optional<String> outputFile;

  private final HBaseAdminWrapper wrapper;

  @Inject
  public CompactRegionsFromFileJob(HBaseAdminWrapper wrapper) {
    this.wrapper = wrapper;
  }

  @Override
  public void run() {
    try {
      Map<HRegionInfo, ServerName> regions = wrapper.getAllRegionInfos(true);

      if (inputFile.isPresent()) {
        for (Entry<HRegionInfo, ServerName> entry : IO.getRegionsExistingInFile(regions.keySet(), inputFile.get()).entrySet()) {
          wrapper.get().majorCompact(entry.getKey().getRegionName());
        }

      }

      if (outputFile.isPresent()) {
        IO.writeRegionsToFile(regions, outputFile.get());
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

}
