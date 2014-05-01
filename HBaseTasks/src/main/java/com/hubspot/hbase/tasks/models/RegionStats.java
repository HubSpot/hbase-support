package com.hubspot.hbase.tasks.models;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.ComparisonChain;

import javax.annotation.Nullable;


@SuppressWarnings("serial")
public class RegionStats implements Serializable, Comparable<RegionStats> {

  private final HRegionInfo regionInfo;
  private final ServerName serverName;
  private Optional<Long> size = Optional.absent();
  private Optional<Long> modificationTime = Optional.absent();
  private Optional<Double> loadWeight = Optional.absent();
  private Optional<List<RegionHdfsInfo>> serverStorage = Optional.absent();

  @JsonCreator
  public RegionStats(@JsonProperty("regionInfo") HRegionInfo regionInfo, @JsonProperty("serverName") ServerName serverName, @JsonProperty("hfileCount") int hFileCount) {
    this(regionInfo, serverName);
  }

  public RegionStats(final HRegionInfo regionInfo, final ServerName serverName) {
    this.regionInfo = regionInfo;
    this.serverName = serverName;
  }

  public void setSize(final long size) {
    this.size = Optional.of(size);
  }

  public void setModificationTime(final long modificationTime) {
    this.modificationTime = Optional.of(modificationTime);
  }

  public ServerName getServerName() {
    return serverName;
  }

  public HRegionInfo getRegionInfo() {
    return regionInfo;
  }

  public long getSize() {
    return size.get();
  }

  public boolean hasSize() {
    return size.isPresent();
  }

  public boolean hasModificationTime() {
    return modificationTime.isPresent();
  }

  public long getModificationTime() {
    return modificationTime.get();
  }

  public boolean hasLoadWeight() {
    return loadWeight.isPresent();
  }

  public double getLoadWeight() {
    return loadWeight.get();
  }

  public void setLoadWeight(final double loadWeight) {
    this.loadWeight = Optional.of(loadWeight);
  }

  public void setServerStorage(final List<RegionHdfsInfo> storage) {
    this.serverStorage = Optional.of(storage);
  }

  public void addServerStorage(final RegionHdfsInfo storage) {
    final List<RegionHdfsInfo> hdfsInfoList = this.serverStorage.or(Lists.<RegionHdfsInfo>newArrayList());
    hdfsInfoList.add(storage);
    this.serverStorage = Optional.of(hdfsInfoList);
  }

  public boolean hasServerStorage() {
    return serverStorage.isPresent();
  }

  public List<RegionHdfsInfo> getServerStorage() {
    return serverStorage.get();
  }

  public long transferCost(final ServerName targetServer) {
    if (!serverStorage.isPresent()) return 0L;
    long total = 0L;
    for (final RegionHdfsInfo hdfsInfo : serverStorage.get()) {
      total += hdfsInfo.transferCost(targetServer);
    }
    return total;
  }

  @JsonIgnore
  public int getHFileCount() {
    return serverStorage.isPresent() ? serverStorage.get().size() : 0;
  }

  public double localityFactor() {
    return localityFactor(serverName);
  }
  
  public double localityFactor(final ServerName targetServer) {
    Preconditions.checkState(size.isPresent(), "Need size information to get locality factor.");
    return size.get() == 0L ? 1.0 : ((double) (size.get() - transferCost(targetServer))) / size.get();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
            .add("regionInfo", regionInfo)
            .add("size", size)
            .add("modificationTime", modificationTime)
            .add("serverName", serverName)
            .add("loadWeight", loadWeight)
	          .add("serverStorage", serverStorage)
            .toString();
  }

  public static Comparator<RegionStats> getSizeComparator() {
    return new Comparator<RegionStats>() {
      @Override
      public int compare(RegionStats arg0, RegionStats arg1) {
        return arg0.size.or(0L).compareTo(arg1.size.or(0L));
      }
    };
  }

  public static Comparator<RegionStats> getLoadComparator() {
    return new Comparator<RegionStats>() {
      @Override
      public int compare(final RegionStats o1, final RegionStats o2) {
        return o1.loadWeight.or(0d).compareTo(o2.loadWeight.or(0d));
      }
    };
  }

  public static Comparator<RegionStats> getDateComparator() {
    return new Comparator<RegionStats>() {
      @Override
      public int compare(final RegionStats firstRegionStats, final RegionStats secondRegionStats) {
       return firstRegionStats.modificationTime.or(0L).compareTo(secondRegionStats.modificationTime.or(0L));
      }
    };
  }

  public void updateWith(final RegionStats otherRegionStats) {
    Preconditions.checkArgument(Objects.equal(regionInfo, otherRegionStats.regionInfo));
    Preconditions.checkArgument(Objects.equal(serverName, otherRegionStats.serverName));

    this.loadWeight = this.loadWeight.or(otherRegionStats.loadWeight);
    this.size = this.size.or(otherRegionStats.size);
    this.modificationTime = this.modificationTime.or(otherRegionStats.modificationTime);
    this.serverStorage = this.serverStorage.or(otherRegionStats.serverStorage);
  }

  public static Function<HRegionInfo, RegionStats> regionInfoToStats(final ServerName serverName) {
    return new Function<HRegionInfo, RegionStats>() {
      @Override
      public RegionStats apply(final HRegionInfo input) {
        return new RegionStats(input, serverName);
      }
    };
  }

  public static final Function<RegionStats, HRegionInfo> REGION_STATS_TO_INFO = new Function<RegionStats, HRegionInfo>() {
    @Override
    public HRegionInfo apply(final RegionStats input) {
      return input.getRegionInfo();
    }
  };

  public static final Function<RegionStats, ServerName> REGION_STATS_TO_SERVER = new Function<RegionStats, ServerName>() {
    @Override
    public ServerName apply(final RegionStats input) {
      return input.getServerName();
    }
  };

  public static final Function<RegionStats, String> REGION_STATS_TO_TABLE_NAME = new Function<RegionStats, String>() {
    @Override
    public String apply(@Nullable final RegionStats input) {
      return input == null ? null : input.getRegionInfo().getTableNameAsString();
    }
  };

  public static Multimap<ServerName, RegionStats> regionInfoToStats(final Multimap<ServerName, HRegionInfo> input) {
    final Multimap<ServerName, RegionStats> result = ArrayListMultimap.create();
    for (final ServerName serverName : input.keySet()) {
      result.putAll(serverName, Iterables.transform(input.get(serverName), regionInfoToStats(serverName)));
    }
    return result;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(regionInfo, serverName);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof RegionStats)) return false;
    return Objects.equal(((RegionStats)obj).regionInfo, regionInfo) && Objects.equal(((RegionStats)obj).serverName, serverName);
  }

  @Override
  public int compareTo(final RegionStats regionStats) {
    return ComparisonChain.start()
        .compare(regionInfo, regionStats.regionInfo)
        .compare(serverName, regionStats.serverName)
        .result();
  }
}
