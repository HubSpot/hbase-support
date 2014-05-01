package com.hubspot.hbase.tasks.compaction;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.google.common.collect.Maps;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.hubspot.hbase.tasks.models.RegionStats;

public class TransitionWatcher implements Runnable {
  private static final Log LOG = LogFactory.getLog(TransitionWatcher.class);
  
  private final SlowCompactionManager slowCompactionManager;
  private final HBaseAdmin admin;
  private final Map<String, HRegionInfo> encodedToRegions;
  private final Map<HRegionInfo, ServerName> transitions;
  private final Map<HRegionInfo, RegionStats> regionStats;
	private final CountDownLatch stopped = new CountDownLatch(1);

  private TransitionWatcher(final HBaseAdmin admin, final SlowCompactionManager slowCompactionManager, final Iterable<RegionStats> regions, final Map<HRegionInfo, ServerName> transitions) {
    this.slowCompactionManager = slowCompactionManager;
    this.admin = admin;
    this.transitions = transitions;
    
    encodedToRegions = Maps.newHashMap();
    regionStats = Maps.newHashMap();
    
    for (final RegionStats region : regions) {
      encodedToRegions.put(region.getRegionInfo().getEncodedName(), region.getRegionInfo());
      regionStats.put(region.getRegionInfo(), region);
    }
  }

  public static Stoppable getStartWatcherThread(HBaseAdmin admin, SlowCompactionManager manager, Collection<RegionStats> regionStats, Map<HRegionInfo, ServerName> transitions) {
		final TransitionWatcher runnable = new TransitionWatcher(admin, manager, regionStats, transitions);
    final Thread compactionThread = new Thread(runnable);
    compactionThread.setName("compaction-thread-watcher");
    compactionThread.setDaemon(true);

		compactionThread.start();

		return new Stoppable() {
			@Override
			public void stop() {
				runnable.stopped.countDown();
			}
		};
  }
  
  @Override
  public void run() {
    try {
      slowCompactionManager.startCompactionQueue();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    
    Set<String> regionsInTransition = Collections.emptySet();
    while (stopped.getCount() > 0) {
      try {
        final Set<String> newRegions = admin.getClusterStatus().getRegionsInTransition().keySet();
        for (final String region : Sets.difference(regionsInTransition, newRegions)) {
          final HRegionInfo regionInfo = encodedToRegions.get(region);
          try {
            if (transitions.containsKey(regionInfo)) {
              slowCompactionManager.compactRegionOnServerIfNecessary(regionStats.get(encodedToRegions.get(region)), transitions.get(regionInfo));
            } else if (encodedToRegions.containsKey(region)) {
              // this isn't one of ours, but we might as well compact it
              System.out.println(String.format("Detected a move that we didn't trigger: %s.  Attempting to compact.", region));
              slowCompactionManager.compactRegionIfNecessary(regionStats.get(encodedToRegions.get(region)));
            }
          } catch (Exception e) {
            LOG.error(String.format("Error trying to compact region: %s [%s]", region, regionInfo), e);
          }
        }
        regionsInTransition = newRegions;
        Thread.sleep(1000);
      } catch (Throwable t) {
        LOG.error(t, t);
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          throw Throwables.propagate(e);
        }
      }
    }
  }

	public interface Stoppable {
		void stop();
	}
}
