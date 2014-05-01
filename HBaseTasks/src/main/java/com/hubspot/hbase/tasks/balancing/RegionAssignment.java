package com.hubspot.hbase.tasks.balancing;

import com.google.common.base.Objects;
import com.hubspot.hbase.tasks.models.RegionStats;
import org.apache.hadoop.hbase.ServerName;

public class RegionAssignment {
  private final RegionStats regionStats;
  private final ServerName newServer;

  public RegionAssignment(final RegionStats regionStats, final ServerName newServer) {
    this.regionStats = regionStats;
    this.newServer = newServer;
  }

  public RegionStats getRegionStats() {
    return regionStats;
  }

  public ServerName getNewServer() {
    return newServer;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(regionStats, newServer);
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof RegionAssignment)) return false;
    final RegionAssignment other = (RegionAssignment) o;
    return Objects.equal(other.regionStats, regionStats) && Objects.equal(other.newServer, newServer);
  }

  @Override
  public String toString() {
    return String.format(String.format("RegionAssignment{%s -> %s}", regionStats, newServer));
  }
}