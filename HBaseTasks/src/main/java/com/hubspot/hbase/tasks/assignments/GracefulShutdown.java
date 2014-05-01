package com.hubspot.hbase.tasks.assignments;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import com.google.common.collect.Lists;
import com.hubspot.hbase.tasks.compaction.SlowCompactionManager;
import com.hubspot.hbase.tasks.compaction.TransitionWatcher;
import com.hubspot.hbase.tasks.config.commandargs.ForArg;
import com.hubspot.hbase.tasks.config.commandargs.HBaseTaskOption;
import com.hubspot.hbase.tasks.hdfs.HBaseHdfsInfo;
import com.hubspot.hbase.tasks.hdfs.HdfsLocalityInfo;
import com.hubspot.hbase.tasks.models.RegionStats;
import com.hubspot.hbase.utils.IO;
import org.apache.hadoop.hbase.HBaseAdminWrapper;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GracefulShutdown {
  private final HBaseAdminWrapper hBaseAdminWrapper;
  private final SlowCompactionManager compactor;
  private final HdfsLocalityInfo hdfsLocalityInfo;
  private final HBaseHdfsInfo hbaseHdfsInfo;
  
  @Inject @ForArg(HBaseTaskOption.MAX_TRANSITIONS)
  private Optional<Integer> maxSimultaneousTransitions;
  @Inject @ForArg(HBaseTaskOption.SERVER_NAME)
  private Optional<String> serverName;
  @Inject @ForArg(HBaseTaskOption.COMPACT_POST_MOVE)
  private Optional<Boolean> compactPostMove;
  @Inject @ForArg(HBaseTaskOption.OUTPUT_FILE)
  private Optional<String> outputFile;


  @Inject
  public GracefulShutdown(final HBaseAdminWrapper hBaseAdminWrapper, final SlowCompactionManager compactor, final HdfsLocalityInfo hdfsLocalityInfo, final HBaseHdfsInfo hbaseHdfsInfo) {
    this.hBaseAdminWrapper = hBaseAdminWrapper;
    this.compactor = compactor;
    this.hdfsLocalityInfo = hdfsLocalityInfo;
    this.hbaseHdfsInfo = hbaseHdfsInfo;
  }

  public void gracefullyShutdown() throws Exception {
    final Set<String> servers = HBaseTaskOption.getCommaSeparatedAsList(serverName);
    final Multimap<ServerName, HRegionInfo> serverRegions = hBaseAdminWrapper.getRegionInfosByServer(true);
    
    final List<HRegionInfo> regions = Lists.newArrayList();
    final Map<HRegionInfo, ServerName> currentAssignments = Maps.newHashMap();
    
    final List<String> serverNames = Lists.newArrayList();
    final List<ServerName> encodedServers = Lists.newArrayList();
    
    for (final ServerName server : serverRegions.keySet()) {
      if (servers.contains(server.getHostname())) {
        regions.addAll(serverRegions.get(server));
        
        if (outputFile.isPresent()) {
          for (HRegionInfo region : serverRegions.get(server)) {
            currentAssignments.put(region, server);
          }
        }
      } else {
        serverNames.add(server.getHostname());
        encodedServers.add(server);
      }
    }

    if (regions.isEmpty()) {
      System.out.println(String.format("Could not find server(s) %s from list: %s", servers, serverNames));
      return;
    }

    hBaseAdminWrapper.get().setBalancerRunning(false, true);

    Collections.shuffle(regions);
    Collections.shuffle(encodedServers);

    int i = 0;
    
    Map<HRegionInfo, ServerName> transitions = Maps.newHashMap();
    if (compactPostMove.or(false)) {
      Multimap<ServerName, RegionStats> regionStats = RegionStats.regionInfoToStats(serverRegions);
      hdfsLocalityInfo.annotateRegionsWithLocalityInPlace(regionStats);
      hbaseHdfsInfo.annotateRegionsWithHdfsInfoInPlace(regionStats);
      TransitionWatcher.getStartWatcherThread(hBaseAdminWrapper.get(), compactor, regionStats.values(), transitions);
    }
    
    Optional<OutputStreamWriter> writer = Optional.absent();
    if (outputFile.isPresent()) {
      writer = Optional.of(IO.writerForFile(outputFile.get(), true));
    }

    for (final HRegionInfo region : regions) {
      waitForTransitionRoom(maxSimultaneousTransitions.get());
      final ServerName otherServer = encodedServers.get(i % encodedServers.size());

      try {
        if (compactPostMove.or(false)) {
          transitions.put(region, otherServer);
        }
        System.out.println(String.format("Moving %s::%s -> %s", region.getTableNameAsString(), region.getEncodedName(), otherServer.getHostname()));
        hBaseAdminWrapper.move(region, otherServer);
        
        if (writer.isPresent()) {
          IO.writeRegionAndServer(region, currentAssignments.get(region), writer.get());
          writer.get().flush();
        }
        
      } catch (Throwable t) {
        throw new RuntimeException(t);
      }

      i++;
    }
    
    if (writer.isPresent()) {
      writer.get().close();
    }
    
    if (compactPostMove.or(false)) {
      compactor.awaitCompactions();
    }
  }

  private void waitForTransitionRoom(final int maxTransitions) throws IOException, InterruptedException {
    while (hBaseAdminWrapper.get().getClusterStatus().getRegionsInTransition().size() >= maxTransitions) {
      Thread.sleep(2000);
    }
  }
}
