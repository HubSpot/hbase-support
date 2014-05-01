package com.hubspot.hbase.tasks.workers;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.hubspot.hbase.tasks.jobs.HBaseBalancerJob;

public class HBaseBalancerWorker implements Runnable {
  private final Provider<HBaseBalancerJob> hBaseBalancerJob;

  @Inject
  public HBaseBalancerWorker(final Provider<HBaseBalancerJob> hBaseBalancerJob) {
		this.hBaseBalancerJob = hBaseBalancerJob;
	}

  @Override
  public void run() {
    while (true) {
      hBaseBalancerJob.get().run();
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
