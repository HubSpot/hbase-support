package com.hubspot.hbase.tasks.balancing.annealing;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.hubspot.hbase.tasks.balancing.RegionAssignment;
import com.hubspot.hbase.tasks.models.RegionStats;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.HashMap;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AssignmentConfig {

  private final HashMap<RegionStats, RegionAssignment> assignments;
  private final ImmutableSet<ServerName> servers;
  private Multimap<ServerName, RegionStats> memoizedIndex = null;

  public AssignmentConfig(final Multimap<ServerName, RegionStats> regionInfo, final ImmutableSet<ServerName> servers) {
    final Set<ServerName> tempServers = Sets.newHashSet();
    HashMap<RegionStats, RegionAssignment> currentMap = new HashMap<RegionStats, RegionAssignment>();
    for (final ServerName server : regionInfo.keySet()) {
      for (final RegionStats region : regionInfo.get(server)) {
        currentMap = currentMap.$plus(new Tuple2<RegionStats, RegionAssignment>(region, new RegionAssignment(region, server)));
        tempServers.add(server);
      }
    }
    this.servers = servers;
    this.assignments = currentMap;
  }

  private AssignmentConfig(final HashMap<RegionStats, RegionAssignment> assignments, final ImmutableSet<ServerName> servers) {
    this.assignments = assignments;
    this.servers = servers;
  }

  public ServerName getAssignment(final RegionStats region) {
    return assignments.get(region).get().getNewServer();
  }

  public Multimap<ServerName, RegionStats> getAssignmentsByServer() {
    if (memoizedIndex != null) return memoizedIndex;
    memoizedIndex = ArrayListMultimap.create();
    for (final RegionAssignment assignment : getAssignments()) {
      memoizedIndex.put(assignment.getNewServer(), assignment.getRegionStats());
    }
    return memoizedIndex;
  }

  public Iterable<RegionStats> getAllRegions() {
    return Iterables.transform(getAssignments(), new Function<RegionAssignment, RegionStats>() {
      @Override
      public RegionStats apply(@Nullable final RegionAssignment input) {
        return input == null ? null : input.getRegionStats();
      }
    });
  }

  public Optional<AssignmentConfig> assign(final RegionStats region, final ServerName server) {
    final Option<RegionAssignment> maybeOldAssignment = assignments.get(region);
    final ServerName oldServer = maybeOldAssignment.get().getNewServer();
    if (Objects.equal(oldServer, server)) return Optional.absent();

    final RegionAssignment assignment = new RegionAssignment(region, server);
    return Optional.of(new AssignmentConfig(assignments.$plus(new Tuple2<RegionStats, RegionAssignment>(region, assignment)), servers));
  }

  public Iterable<RegionAssignment> getAssignments() {
    return new Iterable<RegionAssignment>() {
      @Override
      public Iterator<RegionAssignment> iterator() {
        final scala.collection.Iterator<RegionAssignment> iterator = assignments.valuesIterator();
        return new Iterator<RegionAssignment>() {
          @Override
          public boolean hasNext() {
            return iterator.hasNext();
          }

          @Override
          public RegionAssignment next() {
            return iterator.next();
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  public Map<HRegionInfo, ServerName> getResultMap(final AssignmentConfig original) {
    final Map<HRegionInfo, ServerName> result = Maps.newHashMap();
    for (final RegionAssignment assignment : difference(original)) {
      result.put(assignment.getRegionStats().getRegionInfo(), assignment.getNewServer());
    }
    return result;
  }

  public List<RegionAssignment> difference(final AssignmentConfig other) {
    return Lists.newArrayList(Sets.difference(Sets.newHashSet(getAssignments()), Sets.newHashSet(other.getAssignments())));
  }

  public ImmutableSet<ServerName> getServers() {
    return servers;
  }
}
