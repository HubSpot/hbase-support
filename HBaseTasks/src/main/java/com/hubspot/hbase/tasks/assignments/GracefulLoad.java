package com.hubspot.hbase.tasks.assignments;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.common.collect.ComparisonChain;
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
import java.util.*;
import java.util.Map.Entry;

public class GracefulLoad {

  private final HBaseAdminWrapper hBaseAdminWrapper;
  private final SlowCompactionManager slowCompactionManager;
  private final HdfsLocalityInfo hdfsLocalityInfo;
  private final HBaseHdfsInfo hbaseHdfsInfo;
  
  @Inject @ForArg(HBaseTaskOption.SERVER_NAME)
  private Optional<String> serverName;
  @Inject @ForArg(HBaseTaskOption.SOURCE_SERVER_NAME)
  private Optional<String> sourceServerName;
  @Inject @ForArg(HBaseTaskOption.PULL_MOST_LOCAL)
  private Optional<Boolean> pullMostLocal;
  @Inject @ForArg(HBaseTaskOption.COMPACT_POST_MOVE)
  private Optional<Boolean> compactPostMove;
  @Inject @ForArg(HBaseTaskOption.INPUT_FILE)
  private Optional<String> inputFile;
  
  
  @Inject
  public GracefulLoad(final HBaseAdminWrapper hBaseAdminWrapper,
      final SlowCompactionManager slowCompactionManager,
      final HdfsLocalityInfo hdfsLocalityInfo,
      final HBaseHdfsInfo hbaseHdfsInfo) {
    this.slowCompactionManager = slowCompactionManager;
    this.hBaseAdminWrapper = hBaseAdminWrapper;
    this.hdfsLocalityInfo = hdfsLocalityInfo;
    this.hbaseHdfsInfo = hbaseHdfsInfo;
  }

  public void gracefullyLoad() throws Exception {
    HBaseTaskOption.validateConflictingArgs(String.format("May only specify one of %s, %s.", HBaseTaskOption.INPUT_FILE.getShortName(), HBaseTaskOption.SERVER_NAME.getLongName()), inputFile, serverName);
    
    final Map<String, ServerName> servers = hBaseAdminWrapper.getRegionServersByHostName();
    final Map<HRegionInfo, ServerName> regions = hBaseAdminWrapper.getAllRegionInfos(true);
    
    final Multimap<ServerName, RegionStats> partitioned = RegionStats.regionInfoToStats(hBaseAdminWrapper.partitionRegionInfosByServer(regions));
    hbaseHdfsInfo.annotateRegionsWithHdfsInfoInPlace(partitioned);
    hdfsLocalityInfo.annotateRegionsWithLocalityInPlace(partitioned);

    System.out.println("Found a total of regionServers=" + servers.size() + ", emptyRegionServers=" + (servers.size() - partitioned.keySet().size()) + ", regions=" + regions.size());
    
    Map<HRegionInfo, ServerName> transitions;
    if (inputFile.isPresent()) {
      System.out.println("Loading transition plan from inputFile=" + inputFile.get());
      transitions = IO.getRegionsExistingInFile(regions.keySet(), inputFile.get());
    } else {
      System.out.println("Loading regions onto serverName=" + serverName.get());
      transitions = transitionsForSingleServer(partitioned, servers);
    }
    
    hBaseAdminWrapper.get().setBalancerRunning(false, true);
    
    Set<String> transitionedRegionNames = Sets.newHashSet();
    
    if (compactPostMove.or(false)) {
      TransitionWatcher.getStartWatcherThread(hBaseAdminWrapper.get(), slowCompactionManager, partitioned.values(), transitions);
    }
    
    for (Entry<HRegionInfo, ServerName> entry : transitions.entrySet()) {
      if (regions.get(entry.getKey()).equals(entry.getValue())) {
        System.out.println("Region already exists at target.  Skipping.");
        continue;
      }
      
      System.out.println(String.format("Moving %s::%s -> %s", entry.getKey().getTableNameAsString(), entry.getKey().getEncodedName(), entry.getValue().getHostname()));
      hBaseAdminWrapper.move(entry.getKey(), entry.getValue());
      
      transitionedRegionNames.add(entry.getKey().getEncodedName());
      
      waitForTransitionCompletion(transitionedRegionNames);
    }
    
    if (compactPostMove.or(false)) {
      slowCompactionManager.awaitCompactions();
    }
  }
  
  private Map<HRegionInfo, ServerName> transitionsForSingleServer(Multimap<ServerName, RegionStats> partitioned, Map<String, ServerName> servers) throws Exception {
    ServerName targetServer = servers.get(serverName.get());
    ServerName sourceServer = sourceServerName.isPresent() ? servers.get(sourceServerName.get()) : null;
    
    if (targetServer == null) {
      throw new RuntimeException(String.format("targetServer=%s is not online or not recognized by HMaster", serverName));
    }
    
    if (sourceServerName.isPresent() && sourceServer == null) {
      throw new RuntimeException(String.format("sourceServer=%s is not online or not recognized by HMaster", sourceServerName.get()));
    }
    
    int preferredSize = partitioned.size() / servers.size();
    List<RegionStats> toMove = Lists.newArrayListWithCapacity(preferredSize);

    if (sourceServer != null) {
      // If we found a sourceServer, we will take all of its regions
      System.out.println("Moving all regions from sourceServer=" + sourceServerName.get() + " to targetServer=" + serverName.get());
      toMove.addAll(partitioned.get(sourceServer));
    } else {
      // If no source server, we'll pick from all the other active RS
      System.out.println("Targetting preferredSize=" + preferredSize + " for new regionserver");
      
      for (ServerName server : partitioned.keySet()) {
        // don't include regions already on the target server for inclusion in the transition plan
        if (server.equals(targetServer)) {
          continue;
        }
        
        Collection<RegionStats> regions = partitioned.get(server);
        if (pullMostLocal.or(false)) {
          // Just add all, we will sort them all later
          toMove.addAll(regions);
        } else {
          // get those on this regionserver who are most local to the target
          List<RegionStats> regionsForServer = Lists.newArrayList(regions);
          if (regionsForServer.size() > preferredSize) {
            sortRegionsByLocality(regionsForServer, targetServer);
            toMove.addAll(regionsForServer.subList(0, regionsForServer.size() - preferredSize));
          }
        }
      }
    }
    
    // Final sort, then pull only the amount we need
    sortRegionsByLocality(toMove, targetServer);
    if (!sourceServerName.isPresent() && toMove.size() > preferredSize) {
      toMove = toMove.subList(0, preferredSize);
    }
    
    Map<HRegionInfo, ServerName> result = Maps.newHashMap();
    for (RegionStats region : toMove) {
      result.put(region.getRegionInfo(), targetServer);
    }
    
    System.out.println("Will move " + result.size() + " regions to targetServer=" + targetServer.getHostname());
    
    return result;
  }
  
  private void sortRegionsByLocality(List<RegionStats> regions, final ServerName targetServer) {
    Collections.sort(regions, new Comparator<RegionStats>() {
      @Override
      public int compare(final RegionStats o1, final RegionStats o2) {
        return ComparisonChain.start()
                .compare(o2.localityFactor(targetServer), o1.localityFactor(targetServer))
                .compare(o1.localityFactor(), o2.localityFactor())
                .compare(o1.getModificationTime(), o2.getModificationTime())
                .result();
      }
    });
  }
  

  private void waitForTransitionCompletion(Set<String> transitioned) throws IOException, InterruptedException {
    while (!Sets.intersection(transitioned, hBaseAdminWrapper.get().getClusterStatus().getRegionsInTransition().keySet()).isEmpty()) {
      Thread.sleep(2000);
    }
  }
}
