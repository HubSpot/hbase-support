package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

public class MergeTools {
  /**
   * Functions pulled from org.apache.hadoop.hbase.regionserver.HRegion
   */
  public static final Log LOG = LogFactory.getLog(MergeTools.class);

  /**
   * Merge two HRegions.  The regions must be adjacent and must not overlap.
   *
   * @param srcA
   * @param srcB
   * @return new merged HRegion
   * @throws IOException
   * @throws InterruptedException
   */
  public static HRegion mergeContiguous(final HRegion[] regions, boolean compact)
          throws IOException, InterruptedException {

    // Ensure they are of all the same table
    String tableName = null;
    for (HRegion region : regions) {
      if (tableName == null) {
        tableName = region.getRegionInfo().getTableNameAsString();
      } else if (!tableName.equals(region.getRegionInfo().getTableNameAsString())) {
        throw new IllegalArgumentException("Regions do not belong to the same table");
      }
    }

    // Sort them so we can find the right start and end keys
    Arrays.sort(regions, new Comparator<HRegion>() {

      @Override
      public int compare(HRegion left, HRegion right) {
        if (left.getStartKey() == null) {
          if (right.getStartKey() == null) {
            throw new IllegalArgumentException("Cannot merge two regions with null start key");
          } else {
            return -1;
          }
        } else if (right.getStartKey() == null) {
          return 1;
        } else {
          return left.comparator.compareRows(left.getStartKey(), 0, left.getStartKey().length, right.getStartKey(), 0, right.getStartKey().length);
        }
      }
    });

    // Ensure contiguous.  That is, the end key of one region equals the start of the next
    for (int i = 0; i < regions.length - 1; i++) {
      if (Bytes.compareTo(regions[i].getEndKey(), regions[i+1].getStartKey()) != 0) {
        throw new IllegalArgumentException("Cannot merge non-contiguous regions");
      }
    }

    final FileSystem fs = regions[0].getFilesystem();

    // Make sure each region's cache is empty

    for (HRegion region : regions) {
      if (region.flushcache() && !compact) {
        LOG.debug("Flush resulted in new storefile.  Must compact.");
        compact = true;
      }

      if (!compact && tooManyStoreFiles(region)) {
        LOG.debug("Too many storefiles in a region.  Must compact.");
        compact = true;
      }
    }

    // Compact each region so we only have one store file per family
    if (compact) {
      ExecutorService executorService = Executors.newFixedThreadPool(regions.length);
      for (final HRegion region : regions) {
        executorService.execute(new Runnable() {

          @Override
          public void run() {
            try {
              region.compactStores(true);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Files for region: " + region);
                listPaths(fs, region.getRegionDir());
              }
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        });
      }

      executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }

    Configuration conf = regions[0].getConf();
    HTableDescriptor tabledesc = regions[0].getTableDesc();
    HLog log = regions[0].getLog();
    Path tableDir = regions[0].getTableDir();

    // Since we have ensured the regions are sorted and contiguous, we can just take the start of first and end of the last
    final byte[] startKey = regions[0].getStartKey();
    final byte[] endKey = regions[regions.length - 1].getEndKey();

    HRegionInfo newRegionInfo = new HRegionInfo(tabledesc.getName(), startKey, endKey);
    LOG.info("Creating new region " + newRegionInfo.toString());

    String encodedName = newRegionInfo.getEncodedName();
    Path newRegionDir = HRegion.getRegionDir(regions[0].getTableDir(), encodedName);
    if(fs.exists(newRegionDir)) {
      throw new IOException("Cannot merge; target file collision at " +
              newRegionDir);
    }
    fs.mkdirs(newRegionDir);

    LOG.info("starting merge of " + regions.length +
            " regions into new region " + newRegionInfo.toString() +
            " with start key <" + Bytes.toStringBinary(startKey) + "> and end key <" +
            Bytes.toStringBinary(endKey) + ">");

    // Move HStoreFiles under new region directory
    Map<byte [], List<StoreFile>> byFamily = new TreeMap<byte [], List<StoreFile>>(Bytes.BYTES_COMPARATOR);
    for (HRegion region : regions) {
      byFamily = filesByFamily(byFamily, region.close());
    }

    for (Map.Entry<byte [], List<StoreFile>> es : byFamily.entrySet()) {
      byte [] colFamily = es.getKey();
      HRegion.makeColumnFamilyDirs(fs, tableDir, newRegionInfo, colFamily);
      // Because we compacted the source regions we should have no more than two
      // HStoreFiles per family and there will be no reference store
      List<StoreFile> srcFiles = es.getValue();
      if (srcFiles.size() == 2) {
        long seqA = srcFiles.get(0).getMaxSequenceId();
        long seqB = srcFiles.get(1).getMaxSequenceId();
        if (seqA == seqB) {
          // Can't have same sequenceid since on open of a store, this is what
          // distingushes the files (see the map of stores how its keyed by
          // sequenceid).
          throw new IOException("Files have same sequenceid: " + seqA);
        }
      }

      for (StoreFile hsf: srcFiles) {
        StoreFile.rename(fs, hsf.getPath(), StoreFile.getUniqueFile(fs, Store.getStoreHomedir(tableDir, newRegionInfo.getEncodedName(), colFamily)));
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Files for new region");
      listPaths(fs, newRegionDir);
    }

    HRegion dstRegion = HRegion.newHRegion(tableDir, log, fs, conf, newRegionInfo, regions[0].getTableDesc(), null);

    int readCount = 0;
    int writeCount = 0;
    for (HRegion region : regions) {
      readCount += region.readRequestsCount.get();
      writeCount += region.writeRequestsCount.get();
    }

    dstRegion.readRequestsCount.set(readCount);
    dstRegion.writeRequestsCount.set(writeCount);

    dstRegion.initialize();
    //dstRegion.compactStores();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Files for new region");
      listPaths(fs, dstRegion.getRegionDir());
    }

    for (HRegion region : regions) {
      HFileArchiver.archiveRegion(fs, FSUtils.getRootDir(region.getConf()), region.getTableDir(), region.getRegionDir());
    }

    LOG.info("merge completed. New region is " + dstRegion);

    return dstRegion;
  }

  private static boolean tooManyStoreFiles(HRegion region) {
    for (Entry<byte[], Store> store : region.getStores().entrySet()) {
      if (store.getValue().getNumberOfStoreFiles() > 1) {
        return true;
      }
    }

    return false;
  }

  @SuppressWarnings("deprecation")
  private static void listPaths(FileSystem fs, Path dir) throws IOException {
    if (LOG.isDebugEnabled()) {
      FileStatus[] stats = FSUtils.listStatus(fs, dir, null);
      if (stats == null || stats.length == 0) {
        return;
      }
      for (int i = 0; i < stats.length; i++) {
        String path = stats[i].getPath().toString();
        if (stats[i].isDir()) {
          LOG.debug("d " + path);
          listPaths(fs, stats[i].getPath());
        } else {
          LOG.debug("f " + path + " size=" + stats[i].getLen());
        }
      }
    }
  }

  /*
   * Fills a map with a vector of store files keyed by column family.
   * @param byFamily Map to fill.
   * @param storeFiles Store files to process.
   * @param family
   * @return Returns <code>byFamily</code>
   */
  private static Map<byte [], List<StoreFile>> filesByFamily(
          Map<byte [], List<StoreFile>> byFamily, List<StoreFile> storeFiles) {
    for (StoreFile src: storeFiles) {
      byte [] family = src.getFamily();
      List<StoreFile> v = byFamily.get(family);
      if (v == null) {
        v = new ArrayList<StoreFile>();
        byFamily.put(family, v);
      }
      v.add(src);
    }
    return byFamily;
  }
}
