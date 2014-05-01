package com.hubspot.hbase.tasks.jobs;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.hubspot.hbase.tasks.config.commandargs.ForArg;
import com.hubspot.hbase.tasks.config.commandargs.HBaseTaskOption;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseAdminWrapper;

import java.util.Set;

public class FlushMemstoreJob implements Runnable {
  static final Log LOG = LogFactory.getLog(FlushMemstoreJob.class);

  public static final String SHORT_OPT = "flushMemstore";
  public static final String LONG_OPT = "flushMemstore";
  public static final String DESCRIPTION = "Flush memstore for the table specified";

  @Inject
  @ForArg(HBaseTaskOption.TABLE)
  private Optional<String> tableToFlush;

  private HBaseAdminWrapper wrapper;

  @Inject
  public FlushMemstoreJob(final HBaseAdminWrapper wrapper) {
    this.wrapper = wrapper;
  }

  @Override
  public void run() {
    final Set<String> tables = Sets.newHashSet();
    if (tableToFlush.get().contains(",")) {
      for (String server : tableToFlush.get().split(",")) {
        tables.add(server);
      }
    } else {
      tables.add(tableToFlush.get());
    }

    for (String table : tables) {
      LOG.info(String.format("Flushing %s", table));
      try {
        wrapper.flush(table);
      } catch (Exception e) {
        LOG.fatal(String.format("Error flushing %s", table), e);
        throw Throwables.propagate(e);
      }

    }

  }

}
