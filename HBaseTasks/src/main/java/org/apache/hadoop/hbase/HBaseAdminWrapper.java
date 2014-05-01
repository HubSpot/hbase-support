package org.apache.hadoop.hbase;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription.Type;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

public class HBaseAdminWrapper {
  private final HBaseAdmin admin;
  private final Method getCatalogTracker;
  private final Method cleanupCatalogTracker;

  public HBaseAdminWrapper(final HBaseAdmin admin) {
    this.admin = admin;

    try {
      getCatalogTracker = admin.getClass().getDeclaredMethod("getCatalogTracker");
      getCatalogTracker.setAccessible(true);
      cleanupCatalogTracker = admin.getClass().getDeclaredMethod("cleanupCatalogTracker", CatalogTracker.class);
      cleanupCatalogTracker.setAccessible(true);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public HBaseAdmin get() {
    return admin;
  }

  public CatalogTracker getCatalogTracker() {
    try {
      return (CatalogTracker)getCatalogTracker.invoke(admin);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public void cleanupCatalogTracker(final CatalogTracker catalogTracker) {
    if (catalogTracker == null) return;
    try {
      cleanupCatalogTracker.invoke(admin, catalogTracker);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public Set<String> getDisabledTables() throws IOException {
    final Set<String> disabledTables = Sets.newHashSet();
    for (final HTableDescriptor descriptor : get().listTables()) {
      if (get().isTableDisabled(descriptor.getName())) {
        disabledTables.add(descriptor.getNameAsString());
      }
    }
    return disabledTables;
  }

  public Set<String> getEnabledTables() throws IOException {
    final Set<String> enabledTables = Sets.newHashSet();
    for (final HTableDescriptor descriptor : get().listTables()) {
      if (!get().isTableDisabled(descriptor.getName())) {
        enabledTables.add(descriptor.getNameAsString());
      }
    }
    return enabledTables;
  }

  private Map<HRegionInfo, ServerName> getAllRegionInfos(final Set<String> excludeTables) throws IOException {
    CatalogTracker catalogTracker = null;
    try {
      catalogTracker = getCatalogTracker();
      return MetaReader.fullScan(catalogTracker, excludeTables, true);
    } finally {
      cleanupCatalogTracker(catalogTracker);
    }
  }

  public Map<HRegionInfo, ServerName> getRegionInfosForTable(final String table) throws IOException {
    final Set<String> otherTables = Sets.newHashSet();
    for  (final HTableDescriptor descriptor : get().listTables()) {
      if (!table.equalsIgnoreCase(descriptor.getNameAsString())) {
        otherTables.add(descriptor.getNameAsString());
      }
    }
    return getAllRegionInfos(otherTables);
  }

  public Map<HRegionInfo, ServerName> getAllRegionInfos(final boolean excludeDisabled) throws IOException {
    return getAllRegionInfos(excludeDisabled ? getDisabledTables() : Collections.<String>emptySet());
  }

  public Multimap<ServerName, HRegionInfo> getRegionInfosForTableByServer(final String table) throws IOException {
    return Multimaps.invertFrom(Multimaps.forMap(getRegionInfosForTable(table)), ArrayListMultimap.<ServerName, HRegionInfo>create());
  }

  public Multimap<ServerName, HRegionInfo> getRegionInfosByServer(final boolean excludeDisabled) throws IOException {
    return partitionRegionInfosByServer(getAllRegionInfos(excludeDisabled));
  }

  public Multimap<ServerName, HRegionInfo> partitionRegionInfosByServer(final Map<HRegionInfo, ServerName> regions) {
    return Multimaps.invertFrom(Multimaps.forMap(regions), ArrayListMultimap.<ServerName, HRegionInfo>create());
  }

  public Set<ServerName> getRegionServers() throws IOException {
    return Sets.newHashSet(admin.getClusterStatus().getServers());
  }

  public Map<String, ServerName> getRegionServersByHostName() throws IOException {
    Map<String, ServerName> hosts = Maps.newHashMap();
    for (ServerName server : getRegionServers()) {
      hosts.put(server.getHostname(), server);
    }

    return hosts;
  }

  public void move(HRegionInfo region, ServerName server) throws IOException {
    admin.move(region.getEncodedNameAsBytes(), Bytes.toBytes(server.getServerName()));
  }

  public void flush(String tableOrRegionName) throws IOException, InterruptedException{
    admin.flush(tableOrRegionName);
  }

  public SnapshotDescription snapshotAtTime(String tableName, long timestamp) throws SnapshotCreationException, IllegalArgumentException, IOException {
    SnapshotDescription desc = SnapshotDescription.newBuilder()
            .setTable(tableName)
            .setName(String.format("snapshot-%d-%s", timestamp, tableName))
            .setType(Type.FLUSH)
            .build();
    admin.snapshot(desc);

    return desc;
  }

}
