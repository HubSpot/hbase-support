package com.hubspot.hbase.tasks.jobs;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.hubspot.hbase.tasks.balancing.BalanceRunner;


public class HBaseBalancerJob implements Runnable {
  public static final String SHORT_OPT = "runCustomBalancer";
  public static final String LONG_OPT = "runCustomBalancer";
  public static final String DESCRIPTION = "Run the custom balancer.";

  private final BalanceRunner balanceRunner;

  @Inject
  public HBaseBalancerJob(final BalanceRunner balanceRunner) {
    this.balanceRunner = balanceRunner;
  }

  @Override
  public void run() {
    try {
      balanceRunner.runBalancer();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
