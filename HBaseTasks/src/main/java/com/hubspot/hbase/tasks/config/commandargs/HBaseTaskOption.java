package com.hubspot.hbase.tasks.config.commandargs;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;

import java.util.Set;

public enum HBaseTaskOption {
  ALL_REGIONS(Boolean.class, "allRegions", "allRegions", "JOB: majorCompactTable, If true, then compact every region in the table.", false),
  ACTUALLY_RUN(Boolean.class, "actuallyRun", "actuallyRun", "JOBS: runCustomBalancer, mergeTableRegions. If true, then actually apply the result of the balancer to the cluster. For mergeTableRegions, actually execute the merges [default: false]", false),
  MAX_TIME_MILLIS(Long.class, "maxTimeMillis", "maxTimeMillis", "The number of milliseconds for which the operation should go on for. [Default: 600000]", 600000L),
  MAX_TRANSITIONS_PER_MINUTE(Integer.class, "maxTransitionsPerMinute", "maxTransitionsPerMinute", "JOB: runCustomBalancer. The max number of transitions we should try to attempt in a minute. [Default: 75]", 75),
  BALANCE_OPTIMIZATION_TIME_SECONDS(Integer.class, "optimizationTimeSeconds", "optimizationTimeSeconds", "JOB: runCustomBalancer. The max number of seconds we can spend optimizing regions. [Default: 30]", 30),
  MAX_TRANSITIONS(Integer.class, "maxTransitions", "maxTransitions", "JOBS: runCustomBalancer,gracefulShutdown. The maximum number of simultaneous transitions the master should be performing at any one time. [Default: 5]", 5),
  SERVER_NAME("serverName", "serverName", "JOBS: gracefulShutdown, gracefulLoad. Specifies which server to gracefully shutdown or load."),
  OUTPUT_FILE("outputFileName", "outputFileName", "JOB: runCustomBalancer. If provided, export balancing data to file for offline processing."),
  INPUT_FILE("inputFileName", "inputFileName", "JOB: runCustomBalancer. If provided, use file for getting information rather than calling hbase."),
  COST_FUNCTION("costFunction", "costFunction", "JOB: runCustomBalancer. A JSON structure with keys of DataSizeCost, LoadCost, LocalityCost, RegionCountCost, TransitionCost, ProximateRegionKeyCost and values of relative weights."),
  PERTURBER(String.class, "perturber", "perturber", "JOB: runCustomBalancer. Which perturber to apply to try to optimize. One of CombinedPerturber, SomewhatGreedyPerturber, GreedyPerturber, TableGreedyPerturber or RandomPerturber", "TableGreedyPerturber"),
  REVERSE_LOCALITY(Boolean.class, "reverseLocality", "reverseLocality", "JOB: majorCompactTable. If true, then compact in reverse locality order.", false),
  COMPACTION_QUIET_TIME(Integer.class, "compactionQuietTime", "compactionQuietTime", "JOB: majorCompactTable. Amount of seconds to wait between compactions.", 5),
  SIMULTANEOUS_COMPACTIONS(Integer.class, "compactionQueueSize", "compactionQueueSize", "JOB: majorCompactTable. Amount of simultaneous compactions each servers should target.", 2),
  SOURCE_SERVER_NAME("source", "sourceServerName", "JOB: gracefulLoad. Specifies which server to pull regions FROM (I.E. drain regions from one server into another)."),
  PULL_MOST_LOCAL(Boolean.class, "pullMostLocal", "pullMostLocal", "JOB: gracefulLoad. When true, will load the target server with the regions most local to it.  Great for bringing up a RS that died. [Default: false]", false),
  ROWKEY_FILTER("rowKeyFilter", "rowKeyFilter", "JOB: findDeletionEdits. The row key by which to filter WAL edits."),
  REGION_MERGE_FACTOR(Integer.class, "factor", "regionMergeFactor", "The number of regions to combine into 1.  I.E. if you specify 2, number of regions will be cut in half.  5 would reduce by a factor of 5.", 2),
  COMPACT_THRESHOLD(Double.class, "compactThreshold", "compactThresion", "JOB: gracefulLoad, runCustomBalancer.  Will skip compacting a region if its locality is greater than this threshold.", Double.MAX_VALUE),
  ROWKEY_FORMATTER("rowKeyFormatter", "rowKeyFormatter", "JOB: findDeletionEdits. Rough rowkey printing hack i for int, l for long. Caps for rotate left (ex contacts is Il)"),
  BALANCE_STAGNATE_ITER(Integer.class, "stagnateIterations", "stagnateIterations", "JOB: runCustomBalancer. The number of iterations to run while not getting any improvement before giving up. [DEFAULT: 100]", 100),
  TABLE("table", "table", "The name of the table to operate on."),
  COMPACTION_TYPE(String.class, "compactionType", "compactionType", "Type of compaction to run: minor or major", "major"),
  COMPACT_POST_MOVE(Boolean.class, "compactPostMove", "compactPostMove", "If true, automatically compact a region after moving to another server, unless @compactThreshold is reached.", false),
  EMPTY_REGIONS_ONLY(Boolean.class, "emptyRegionsOnly", "emptyRegionsOnly", "JOB: mergeRegionsInTable. If true, merger will only merge empty regions with a nearby non-empty region", false);

  private final Class<?> targetClass;
  private final String shortName;
  private final String longName;
  private final String description;
  private final Object defaultValue;

  private HBaseTaskOption(final String shortName, final String longName, final String description) {
    this(String.class, shortName, longName, description, null);
  }

  private HBaseTaskOption(final Class<?> targetClass, final String shortName, final String longName, final String description, final Object defaultValue) {
    this.targetClass = targetClass;
    this.shortName = shortName;
    this.longName = longName;
    this.description = description;
    this.defaultValue = defaultValue;
  }

  public Object getDefaultValue() {
    return defaultValue;
  }

  public String getDescription() {
    return description;
  }

  public String getLongName() {
    return longName;
  }

  public String getShortName() {
    return shortName;
  }

  public Class<?> getTargetClass() {
    return targetClass;
  }

  public static Set<String> getCommaSeparatedAsList(Optional<String> string) {
    Set<String> values = Sets.newHashSet();
    if (string.isPresent()) {
      if (string.get().contains(",")) {
        for (String server : string.get().split(",")) {
          values.add(server);
        }
      } else {
        values.add(string.get());
      }
    }

    return values;
  }

  public static void validateConflictingArgs(String message, Optional<?>... args) {
    boolean exist = false;
    for (Optional<?> arg : args) {
      if (exist && arg.isPresent()) {
        throw new IllegalArgumentException(message);
      }

      exist |= arg.isPresent();
    }
  }
}
