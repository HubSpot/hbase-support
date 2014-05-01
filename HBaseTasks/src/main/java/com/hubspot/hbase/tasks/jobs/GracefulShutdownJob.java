package com.hubspot.hbase.tasks.jobs;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.hubspot.hbase.tasks.assignments.GracefulShutdown;

public class GracefulShutdownJob implements Runnable {
  public static final String SHORT_OPT = "gracefulShutdown";
  public static final String LONG_OPT = "gracefulShutdown";
  public static final String DESCRIPTION = "Gracefully shutdown the region server.";

  private final GracefulShutdown gracefulShutdown;

  @Inject
  public GracefulShutdownJob(final GracefulShutdown gracefulShutdown) {
    this.gracefulShutdown = gracefulShutdown;
  }

  @Override
  public void run() {
    try {
      gracefulShutdown.gracefullyShutdown();
    } catch (Throwable t) {
      throw Throwables.propagate(t);
    }
  }
}
