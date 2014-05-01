/**
 * Copyright 2009 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.util;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.HConnectionManager.HConnectable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MergeTools;
import org.apache.hadoop.hbase.regionserver.wal.HLog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

/**
 * A non-instantiable class that has a static method capable of compacting
 * a table by merging adjacent regions.
 */
public class HMerge {
  public static String REGION_FACTOR_CONF = "hmerge.region.factor";
  public static int REGION_FACTOR_DEFAULT = 2;
  // TODO: Where is this class used?  How does it relate to Merge in same package?
  static final Log LOG = LogFactory.getLog(HMerge.class);
  static final Random rand = new Random();

  /*
   * Not instantiable
   */
  private HMerge() {
    super();
  }

  /**
   * Scans the table and merges two adjacent regions if they are small. This
   * only happens when a lot of rows are deleted.
   *
   * When merging the META region, the HBase instance must be offline.
   * When merging a normal table, the HBase instance must be online, but the
   * table must be disabled.
   *
   * @param conf        - configuration object for HBase
   * @param fs          - FileSystem where regions reside
   * @param tableName   - Table to be compacted
   * @throws IOException
   * @throws InterruptedException
   */
  public static void merge(Configuration conf, FileSystem fs, final byte [] tableName, final boolean shouldExecute) throws IOException, InterruptedException {
    merge(conf, tableName, true, new FullTableMerger(conf, fs, tableName, shouldExecute));
  }

  /**
   * Scans the table and merges any empty regions with a nearby non-empty regions
   *
   * @param conf        - configuration object for HBase
   * @param fs          - FileSystem where regions reside
   * @param tableName   - Table to be purged of empty regions
   * @throws IOException
   * @throws InterruptedException
   */
  public static void mergeEmptyRegions(Configuration conf, FileSystem fs, final byte [] tableName, final boolean shouldExecute) throws IOException, InterruptedException {
    merge(conf, tableName, true, new EmptyRegionMerger(conf, fs, tableName, shouldExecute));
  }

  /**
   * Scans the table and merges two adjacent regions if they are small. This
   * only happens when a lot of rows are deleted.
   *
   * When merging the META region, the HBase instance must be offline.
   * When merging a normal table, the HBase instance must be online, but the
   * table must be disabled.
   *
   * @param conf        - configuration object for HBase
   * @param tableName   - Table to be compacted
   * @param testMasterRunning True if we are to verify master is down before
   * running merge
   * @param merger      - Instance of a Merger to use to use to merge regions
   * @throws IOException
   * @throws InterruptedException
   */
  public static void merge(Configuration conf, final byte [] tableName, final boolean testMasterRunning, Merger merger)
          throws IOException, InterruptedException {
    boolean masterIsRunning = false;
    if (testMasterRunning) {
      masterIsRunning = HConnectionManager
              .execute(new HConnectable<Boolean>(conf) {
                @Override
                public Boolean connect(HConnection connection) throws IOException {
                  return connection.isMasterRunning();
                }
              });
    }
    if(!masterIsRunning) {
      throw new IllegalStateException(
              "HBase instance must be running to merge a normal table");
    }
    HBaseAdmin admin = new HBaseAdmin(conf);
    try {
      if (!admin.isTableDisabled(tableName)) {
        throw new TableNotDisabledException(tableName);
      }
    } finally {
      admin.close();
    }

    merger.process();
  }

  private static abstract class Merger {
    private final String tableName;
    private final byte[] tableNameBytes;
    private final boolean execute;
    private final HTable metaTable;
    private final ResultScanner metaScanner;
    private final FileSystem fs;
    private final Path tabledir;
    private final HTableDescriptor htd;
    private final HLog hlog;

    protected final Configuration conf;
    protected final long maxFilesize;


    protected Merger(Configuration conf, FileSystem fs, final byte [] tableNameBytes, boolean execute)
            throws IOException {
      this.conf = conf;
      this.fs = fs;
      this.maxFilesize = conf.getLong(HConstants.HREGION_MAX_FILESIZE, HConstants.DEFAULT_MAX_FILE_SIZE);

      this.tableName = Bytes.toString(tableNameBytes);
      this.tableNameBytes = tableNameBytes;
      this.tabledir = new Path(fs.makeQualified(new Path(conf.get(HConstants.HBASE_DIR))), tableName);
      this.htd = FSTableDescriptors.getTableDescriptor(this.fs, this.tabledir);

      Path logdir = new Path(tabledir, "merge_" + System.currentTimeMillis() + HConstants.HREGION_LOGDIR_NAME);
      Path oldLogDir = new Path(tabledir, HConstants.HREGION_OLDLOGDIR_NAME);
      this.hlog = new HLog(fs, logdir, oldLogDir, conf);

      this.execute = execute;
      this.metaTable = new HTable(conf, HConstants.META_TABLE_NAME);
      this.metaScanner = metaTable.getScanner(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    }

    void process() throws IOException, InterruptedException {
      try {
        for (HRegion[] regionsToMerge = next(); regionsToMerge != null; regionsToMerge = next()) {
          if (!merge(regionsToMerge)) {
            return;
          }
        }
      } finally {
        try {
          hlog.closeAndDelete();

        } catch(IOException e) {
          LOG.error(e);
        }
      }
    }

    protected boolean merge(final HRegion[] regions) throws IOException, InterruptedException {
      if (regions.length < 2) {
        LOG.info("only one region - nothing to merge");
        return false;
      }

      if (execute) {
        HRegion mergedRegion = MergeTools.mergeContiguous(regions, false);
        byte[][] names = new byte[regions.length][];
        for (int i = 0; i < regions.length; i++) {
          names[i] = regions[i].getRegionName();
        }

        updateMeta(names, mergedRegion);
      }

      for (HRegion region : regions) {
        region.close();
      }

      return true;
    }

    protected abstract HRegion[] next() throws IOException;

    protected void updateMeta(final byte[][] regionsToDelete, HRegion newRegion) throws IOException {
      for (int r = 0; r < regionsToDelete.length; r++) {
        Delete delete = new Delete(regionsToDelete[r]);
        if (execute) {
          metaTable.delete(delete);
        }
        if(LOG.isDebugEnabled()) {
          LOG.debug("updated columns in row: " + Bytes.toStringBinary(regionsToDelete[r]));
        }
      }

      Put put = new Put(newRegion.getRegionName());
      put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
              Writables.getBytes(newRegion.getRegionInfo()));
      if (execute) {
        metaTable.put(put);
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug("updated columns in row: "
                + Bytes.toStringBinary(newRegion.getRegionName()));
      }
    }

    protected HRegionInfo nextRegion() throws IOException {
      try {
        Result results = getMetaRow();
        if (results == null) {
          return null;
        }
        byte[] regionInfoValue = results.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
        if (regionInfoValue == null || regionInfoValue.length == 0) {
          throw new NoSuchElementException("meta region entry missing " +
                  Bytes.toString(HConstants.CATALOG_FAMILY) + ":" +
                  Bytes.toString(HConstants.REGIONINFO_QUALIFIER));
        }
        HRegionInfo region = Writables.getHRegionInfo(regionInfoValue);
        if (!Bytes.equals(region.getTableName(), this.tableNameBytes)) {
          return null;
        }
        return region;
      } catch (IOException e) {
        e = RemoteExceptionHandler.checkIOException(e);
        LOG.error("meta scanner error", e);
        metaScanner.close();
        throw e;
      }
    }

    /*
     * Check current row has a HRegionInfo.  Skip to next row if HRI is empty.
     * @return A Map of the row content else null if we are off the end.
     * @throws IOException
     */
    protected Result getMetaRow() throws IOException {
      Result currentRow = metaScanner.next();
      boolean foundResult = false;
      while (currentRow != null) {
        String row = Bytes.toStringBinary(currentRow.getRow());
        byte[] regionInfoValue = currentRow.getValue(HConstants.CATALOG_FAMILY,
                HConstants.REGIONINFO_QUALIFIER);
        if (regionInfoValue == null || regionInfoValue.length == 0) {
          currentRow = metaScanner.next();
          continue;
        }

        if (!row.substring(0, row.indexOf(",")).equals(tableName)) {
          currentRow = metaScanner.next();
          continue;
        }
        LOG.info("Row: <" + row + ">");
        foundResult = true;
        break;
      }
      return foundResult ? currentRow : null;
    }

    protected HRegion getInitializedHRegion(HRegionInfo region) throws IOException {
      HRegion currentRegion = HRegion.newHRegion(tabledir, hlog, fs, conf, region, this.htd, null);
      currentRegion.initialize();

      return currentRegion;
    }
  }

  private static class EmptyRegionMerger extends Merger {
    private HRegionInfo latestRegion;

    protected EmptyRegionMerger(Configuration conf, FileSystem fs, byte[] tableName, boolean execute) throws IOException {
      super(conf, fs, tableName, execute);
    }

    @Override
    protected HRegion[] next() throws IOException {

      if (latestRegion == null) {
        latestRegion = nextRegion();
      }

      HRegionInfo lastNonEmptyRegion = null;
      List<HRegion> regionsToMerge = Lists.newArrayList();

      while (latestRegion != null) {
        HRegion region = getInitializedHRegion(latestRegion);

        if (region.getLargestHStoreSize() == 0) {
          LOG.info("Found empty region");
          regionsToMerge.add(region);
        } else {
          if (!regionsToMerge.isEmpty()) {
            // We want to merge all the empties with a nearby non-empty.  If we didn't see
            // a non-empty before this string of empties, just add the current (which we know is not empty)
            // to the end of the list, keeping sorted order
            if (lastNonEmptyRegion != null) {
              LOG.info("Merging " + regionsToMerge.size() + " empty regions with BEFORE region");
              regionsToMerge.add(0, getInitializedHRegion(lastNonEmptyRegion));
            } else {
              LOG.info("Merging " + regionsToMerge.size() + " empty regions with AFTER region");
              regionsToMerge.add(region);
              latestRegion = null; // clear this out so on next call to next() we move forward
            }


            return regionsToMerge.toArray(new HRegion[regionsToMerge.size()]);
          }

          lastNonEmptyRegion = region.getRegionInfo();
        }

        latestRegion = nextRegion();
      }

      // last region was empty.  only possibility is a 1-region table, entirely empty table, or we have a lastNonEmptyRegion
      // handle the last possibility
      if (!regionsToMerge.isEmpty() && lastNonEmptyRegion != null) {
        LOG.info("Merging " + regionsToMerge.size() + " empty regions at end of table");
        regionsToMerge.add(0, getInitializedHRegion(lastNonEmptyRegion));

        return regionsToMerge.toArray(new HRegion[regionsToMerge.size()]);
      }

      return null;
    }

    @Override
    protected void updateMeta(final byte[][] regionsToDelete, HRegion newRegion) throws IOException {
      for (int r = 0; r < regionsToDelete.length; r++) {
        if(latestRegion != null && Bytes.equals(regionsToDelete[r], latestRegion.getRegionName())) {
          LOG.info("Had to set latestRegion to null");
          latestRegion = null;
        }
      }

      super.updateMeta(regionsToDelete, newRegion);
    }
  }
  /**
   * Merges regions across entire table, reducing the total number of regions
   * by some factor, as defined by HMerge.REGION_FACTOR_CONF
   */
  private static class FullTableMerger extends Merger {
    private HRegionInfo latestRegion;

    FullTableMerger(Configuration conf, FileSystem fs, final byte[] tableNameBytes, boolean execute) throws IOException {
      super(conf, fs, tableNameBytes, execute);
      this.latestRegion = null;
    }

    @Override
    protected HRegion[] next() throws IOException {
      List<HRegion> regions = new ArrayList<HRegion>();
      if(latestRegion == null) {
        latestRegion = nextRegion();
      }

      int sizeToMerge = 0;
      int max = conf.getInt(REGION_FACTOR_CONF, REGION_FACTOR_DEFAULT);

      // Limit to a max of @max, limited also to around half of the @maxFileSize
      for (int i = 0; i < max; i++) {
        if (latestRegion != null) {
          HRegion currentRegion = getInitializedHRegion(latestRegion);
          sizeToMerge += currentRegion.getLargestHStoreSize();
          regions.add(currentRegion);

          if (sizeToMerge >= (maxFilesize / 2)) {
            break;
          }
        }

        // dont increment on last iteration
        if (i + 1 < max) {
          latestRegion = nextRegion();
        }
      }

      return regions.toArray(new HRegion[regions.size()]);
    }

    @Override
    protected void updateMeta(final byte[][] regionsToDelete, HRegion newRegion) throws IOException {
      for (int r = 0; r < regionsToDelete.length; r++) {
        if(latestRegion != null && Bytes.equals(regionsToDelete[r], latestRegion.getRegionName())) {
          latestRegion = null;
        }
      }

      super.updateMeta(regionsToDelete, newRegion);
    }
  }
}
