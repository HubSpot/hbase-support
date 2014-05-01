package com.hubspot.hbase.tasks.hdfs;

import com.google.common.base.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.hubspot.hbase.tasks.config.HBaseTasksModule;
import com.hubspot.hbase.tasks.models.RegionStats;
import com.hubspot.hbase.utils.Pair;
import com.hubspot.hbase.utils.ThreadPools;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.FSUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

@Singleton
public class HBaseHdfsInfo {
  private static final Log LOG = LogFactory.getLog(HBaseHdfsInfo.class);

  private static final ExecutorService executorService = ThreadPools.buildDefaultThreadPool(100, "hbase-hdfs-info-%s");
  private final LoadingCache<Pair<HRegionInfo, ServerName>, RegionStats> regionStatsCache;
  private final FileSystem fileSystem;
  private final Path rootPath;
  private final PathFilter pathFilter;

  @Inject
  public HBaseHdfsInfo(final FileSystem fileSystem,
                       @Named(HBaseTasksModule.HBASE_PATH) final Path rootPath) {
    this.fileSystem = fileSystem;
    this.rootPath = rootPath;
    this.regionStatsCache = CacheBuilder.newBuilder()
            .maximumSize(2000)
            .build(new RegionStatsLoader());
    this.pathFilter = new VisibleDirectory(fileSystem);
  }

  public RegionStats getRegionStats(final HRegionInfo regionInfo, final ServerName serverName) {
    return regionStatsCache.getUnchecked(Pair.of(regionInfo, serverName));
  }

  public Function<Map.Entry<HRegionInfo, ServerName>, RegionStats> regionInfoToStatsFunction() {
    return new Function<Entry<HRegionInfo, ServerName>, RegionStats>() {
      @Override
      public RegionStats apply(final Entry<HRegionInfo, ServerName> input) {
        return getRegionStats(input.getKey(), input.getValue());
      }
    };
  }

  public void annotateRegionsWithHdfsInfoInPlace(final Multimap<ServerName, RegionStats> regionStats) throws InterruptedException, ExecutionException {
    List<Future<?>> futures = Lists.newArrayList();

    for (final RegionStats stat : regionStats.values()) {
      futures.add(executorService.submit(new Runnable() {

        @Override
        public void run() {
          stat.updateWith(getRegionStats(stat.getRegionInfo(), stat.getServerName()));
        }
      }));
    }

    for (Future<?> future : futures) {
      future.get();
    }
  }

  private RegionStats loadRegionStats(final HRegionInfo regionInfo, final ServerName serverName) {
    final Path regionPath = new Path(HTableDescriptor.getTableDir(rootPath, regionInfo.getTableName()), regionInfo.getEncodedName());
    final RegionStats result = new RegionStats(regionInfo, serverName);


    try {
      long totalSize = 0;
      for (FileStatus stat : fileSystem.listStatus(regionPath, pathFilter)) {
        try {
          totalSize += fileSystem.getContentSummary(stat.getPath()).getLength();
        } catch (FileNotFoundException ie) {
          LOG.warn(String.format("Could not find file %s, not including in overall size of region", stat.getPath().toString()), ie);
        }
      }
      result.setSize(totalSize);
    } catch (FileNotFoundException e) {
      LOG.info(String.format("Region file doesn't exist: %s", regionPath), e);
      // the region doesn't have data
      result.setSize(0L);
    } catch (IOException e) {
      LOG.warn(String.format("Could not get filesize for region %s", regionInfo.getEncodedName()), e);
    }
    try {
      result.setModificationTime(getNewestModificationTimeRecursive(fileSystem, regionPath));
    } catch (FileNotFoundException e) {
      // the region doesn't have data
      result.setModificationTime(Long.MIN_VALUE);
    } catch (IOException e) {
      LOG.warn(String.format("Could not get latest modification time for region %s", regionInfo.getEncodedName()), e);
    }
    return result;
  }

  private static long getNewestModificationTimeRecursive(FileSystem hdfs, Path path) throws IOException {
    long max = 0;
    FileStatus root = hdfs.getFileStatus(path);
    Queue<FileStatus> files = new ArrayDeque<FileStatus>();
    files.add(root);
    while (files.peek() != null) {
      FileStatus file = files.poll();
      if (file.isDirectory()) {
        files.addAll(Arrays.asList(hdfs.listStatus(file.getPath())));
      } else {
        max = Math.max(file.getModificationTime(), max);
      }
    }
    return max;
  }

  private class RegionStatsLoader extends CacheLoader<Pair<HRegionInfo, ServerName>, RegionStats> {
    @Override
    public RegionStats load(final Pair<HRegionInfo, ServerName> key) throws Exception {
      return loadRegionStats(key.getFirst(), key.getSecond());
    }
  }

  private class VisibleDirectory extends FSUtils.DirFilter {
    public VisibleDirectory(FileSystem arg0) {
      super(arg0);
    }

    @Override
    public boolean accept(Path p) {
      return super.accept(p) && !p.getName().startsWith(".");
    }
  }

}
