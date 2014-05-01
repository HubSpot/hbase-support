package com.hubspot.hbase.tasks.balancing;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import com.hubspot.hbase.tasks.balancing.config.OptimizationModule;
import com.hubspot.hbase.tasks.compaction.SlowCompactionManager;
import com.hubspot.hbase.tasks.compaction.TransitionWatcher;
import com.hubspot.hbase.tasks.config.HBaseTasksModule;
import com.hubspot.hbase.tasks.config.commandargs.ForArg;
import com.hubspot.hbase.tasks.config.commandargs.HBaseTaskOption;
import com.hubspot.hbase.tasks.hdfs.HdfsLocalityInfo;
import com.hubspot.hbase.tasks.load.RegionLoadEstimation;
import com.hubspot.hbase.tasks.models.RegionStats;
import com.hubspot.hbase.tasks.serialization.ObjectMapperSingleton;
import com.hubspot.hbase.tasks.serialization.SerializationModule;
import com.hubspot.hbase.utils.Gzip;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.concurrent.TimedSemaphore;
import org.apache.hadoop.hbase.HBaseAdminWrapper;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Bytes;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class BalanceRunner {

  private final Provider<RegionLoadEstimation> regionLoadEstimation;
  private final Provider<HBaseAdminWrapper> hBaseAdminWrapper;
  private final Provider<SlowCompactionManager> slowCompactionManager;
  private final Provider<HdfsLocalityInfo> hdfsLocalityInfo;
  private final Balancer balancer;

  @Inject
  @ForArg(HBaseTaskOption.ACTUALLY_RUN)
  private Optional<Boolean> actuallyRun;
  @Inject
  @ForArg(HBaseTaskOption.BALANCE_OPTIMIZATION_TIME_SECONDS)
  private Optional<Integer> maxOptimizationTimeSeconds;
  @Inject
  @ForArg(HBaseTaskOption.MAX_TRANSITIONS_PER_MINUTE)
  private Optional<Integer> transitionsPerMinute;
  @Inject
  @ForArg(HBaseTaskOption.MAX_TRANSITIONS)
  private Optional<Integer> maxSimultaneousTransitions;
  @Inject
  @ForArg(HBaseTaskOption.TABLE)
  private Optional<String> tableToBalance;
  @Inject
  @ForArg(HBaseTaskOption.INPUT_FILE)
  private Optional<String> inputFile;
  @Inject
  @Named(OptimizationModule.TOTAL_TRANSITIONS)
  private int totalTransitions;

  @Inject
  public BalanceRunner(final Provider<RegionLoadEstimation> regionLoadEstimation,
                       final Provider<HBaseAdminWrapper> hBaseAdminWrapper,
                       final Provider<SlowCompactionManager> slowCompactionManager,
                       final Provider<HdfsLocalityInfo> hdfsLocalityInfo,
                       @Named(HBaseTasksModule.ACTIVE_BALANCER) final Balancer balancer) {
    this.regionLoadEstimation = regionLoadEstimation;
    this.hBaseAdminWrapper = hBaseAdminWrapper;
    this.slowCompactionManager = slowCompactionManager;
    this.hdfsLocalityInfo = hdfsLocalityInfo;
    this.balancer = balancer;
  }

  public void runBalancer() throws Exception {
    final Meter movedRegionsMeter = Metrics.defaultRegistry().newMeter(getClass(), "movedRegions", "regions", TimeUnit.HOURS);
    Multimap<ServerName, RegionStats> regionInfos = getRegionInfos();

    if (actuallyRun.get()) {
      balancer.setRegionInformation(regionInfos, ImmutableSet.copyOf(hBaseAdminWrapper.get().getRegionServers()));
    } else {
      balancer.setRegionInformation(regionInfos, ImmutableSet.copyOf(regionInfos.keySet()));
    }

    final int numberOfAssignments = totalTransitions;

    final Map<HRegionInfo, ServerName> transitions = balancer.computeTransitions(maxOptimizationTimeSeconds.get(), numberOfAssignments);

    Preconditions.checkState(transitions.size() <= numberOfAssignments, "Got too many assignments from balancer: %s > %s", transitions.size(), numberOfAssignments);

    System.out.println(String.format("Total transitions: %s / %s", transitions.size(), totalTransitions));

    printTransitions(regionInfos, transitions);

    if (!actuallyRun.get()) {
      System.out.println("Since this is a simulation, not continuing on.\n\n");
      return;
    }

    hBaseAdminWrapper.get().get().setBalancerRunning(false, true);

    TimedSemaphore timedSemaphore = new TimedSemaphore(1, TimeUnit.SECONDS, Ints.checkedCast(TimeUnit.MINUTES.toSeconds(transitionsPerMinute.get())));

    final TransitionWatcher.Stoppable stoppable = TransitionWatcher.getStartWatcherThread(hBaseAdminWrapper.get().get(), slowCompactionManager.get(), regionInfos.values(), transitions);

    try {
      Set<String> movedRegions = Sets.newHashSet();
      for (final Map.Entry<HRegionInfo, ServerName> entry : transitions.entrySet()) {
        timedSemaphore.acquire();
        waitForTransitionRoom(maxSimultaneousTransitions.get(), movedRegions);
        System.out.println(String.format("Moving %s::%s TO %s", entry.getKey().getTableNameAsString(), entry.getKey().getEncodedName(), entry.getValue().getHostname()));
        hBaseAdminWrapper.get().get().move(entry.getKey().getEncodedNameAsBytes(), Bytes.toBytes(entry.getValue().getServerName()));
        movedRegionsMeter.mark();
        movedRegions.add(entry.getKey().getEncodedName());
        Thread.sleep(500);
      }

      waitForTransitionRoom(1, movedRegions);

      Thread.sleep(1000);

      System.out.println("Finished transitions. Waiting for all requested compactions to complete.");
    } finally {
      stoppable.stop();
      slowCompactionManager.get().awaitCompactions();
    }
  }

  private Multimap<ServerName, RegionStats> getRegionInfos() throws Exception {
    if (inputFile.isPresent()) {
      byte[] payload = Files.toByteArray(new File(inputFile.get()));
      if (inputFile.get().endsWith(".gz")) {
        payload = Gzip.decompress(payload);
      }
      SerializationModule.fixJacksonModule();
      final List<RegionStats> input = ObjectMapperSingleton.MAPPER.readValue(payload, new TypeReference<List<RegionStats>>() {
      });
      Iterable<RegionStats> resultingInput = input;
      if (tableToBalance.isPresent()) {
        resultingInput = Iterables.filter(input, new Predicate<RegionStats>() {
          @Override
          public boolean apply(@Nullable final RegionStats input) {
            return input != null && input.getRegionInfo().getTableNameAsString().equalsIgnoreCase(tableToBalance.get());
          }
        });
      }
      return Multimaps.index(resultingInput, RegionStats.REGION_STATS_TO_SERVER);
    }

    final Multimap<ServerName, RegionStats> regionInfos = tableToBalance.isPresent() ?
            RegionStats.regionInfoToStats(hBaseAdminWrapper.get().getRegionInfosForTableByServer(tableToBalance.get()))
            : RegionStats.regionInfoToStats(hBaseAdminWrapper.get().getRegionInfosByServer(true));
    return annotate(regionInfos);
  }

  private Multimap<ServerName, RegionStats> annotate(Multimap<ServerName, RegionStats> regionInfos) throws Exception {
    regionLoadEstimation.get().annotateRegionsWithLoadInPlace(regionInfos);
    hdfsLocalityInfo.get().annotateRegionsWithLocalityInPlace(regionInfos);

    return regionInfos;
  }

  private void waitForTransitionRoom(final int maxTransitions, Set<String> movedRegions) throws IOException, InterruptedException {
    while (Sets.intersection(hBaseAdminWrapper.get().get().getClusterStatus().getRegionsInTransition().keySet(), movedRegions).size() >= maxTransitions) {
      Thread.sleep(1000);
    }
  }

  private void printTransitions(Multimap<ServerName, RegionStats> originalInfo, final Map<HRegionInfo, ServerName> transitions) {
    final Map<HRegionInfo, RegionStats> oldAssignments = Maps.newHashMapWithExpectedSize(transitions.size());
    for (final RegionStats regionStats : originalInfo.values()) {
      if (transitions.containsKey(regionStats.getRegionInfo())) {
        oldAssignments.put(regionStats.getRegionInfo(), regionStats);
      }
    }

    final String row = Strings.repeat("=", 60);
    System.out.println(String.format("%s\nOptimal transitions using %s:\n%s\n", row, balancer.getClass().getSimpleName(), row));
    for (final Map.Entry<HRegionInfo, ServerName> transition : transitions.entrySet()) {
      final HRegionInfo region = transition.getKey();
      final ServerName destServer = transition.getValue();
      final ServerName origServer = oldAssignments.get(region).getServerName();
      final String size = oldAssignments.get(region).hasSize() ? FileUtils.byteCountToDisplaySize(oldAssignments.get(region).getSize()) : "N/A";
      System.out.println(String.format("%s::%s\t%s -> %s [%s]", region.getTableNameAsString(), region.getEncodedName(), origServer.getHostname(), destServer.getHostname(), size));
    }
    System.out.println(row);
    System.out.println("\n\n");
  }

  public void setActuallyRun(final Optional<Boolean> actuallyRun) {
    this.actuallyRun = actuallyRun;
  }

  public void setTransitionsPerMinute(final Optional<Integer> transitionsPerMinute) {
    this.transitionsPerMinute = transitionsPerMinute;
  }

  public void setMaxOptimizationTimeSeconds(final Optional<Integer> maxOptimizationTimeSeconds) {
    this.maxOptimizationTimeSeconds = maxOptimizationTimeSeconds;
  }

  public void setMaxSimultaneousTransitions(final Optional<Integer> maxSimultaneousTransitions) {
    this.maxSimultaneousTransitions = maxSimultaneousTransitions;
  }

  public void setTableToBalance(final Optional<String> tableToBalance) {
    this.tableToBalance = tableToBalance;
  }

  public void setInputFile(final Optional<String> inputFile) {
    this.inputFile = inputFile;
  }
}
