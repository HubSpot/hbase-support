package com.hubspot.hbase.utils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPools {
  private ThreadPools() {}

  public static ExecutorService buildDefaultThreadPool(int maxSize, String nameFormat) {
    return new ThreadPoolExecutor(5, maxSize, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>(), new ThreadFactoryBuilder().setDaemon(true).setNameFormat(nameFormat).build());
  }
}
