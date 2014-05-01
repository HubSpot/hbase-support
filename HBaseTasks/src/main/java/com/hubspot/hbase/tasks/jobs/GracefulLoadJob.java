package com.hubspot.hbase.tasks.jobs;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.hubspot.hbase.tasks.assignments.GracefulLoad;

public class GracefulLoadJob implements Runnable {
  public static final String SHORT_OPT = "gracefulLoad";
  public static final String LONG_OPT = "gracefulLoad";
  public static final String DESCRIPTION = "Gracefully load regions onto a region server.";

  private final GracefulLoad gracefulLoad;

  @Inject
  public GracefulLoadJob(final GracefulLoad gracefulStartup) {
    this.gracefulLoad = gracefulStartup;
  }

  @Override
  public void run() {
    try {
      gracefulLoad.gracefullyLoad();
    } catch (Throwable t) {
      throw Throwables.propagate(t);
    }
  }
}
