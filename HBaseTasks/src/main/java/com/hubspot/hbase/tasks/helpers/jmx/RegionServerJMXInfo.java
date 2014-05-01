package com.hubspot.hbase.tasks.helpers.jmx;

import com.google.common.base.Throwables;
import org.apache.commons.pool.KeyedObjectPool;

import java.io.Closeable;
import java.io.IOException;

/**
 * A wrapper around common JMX requests. In order to maximize
 * efficiency and reduce total # of connections, we want to
 * keep a singleton pool of connections.
 * (This also handles the case where in a flakey environment
 * the jmx connections might die and come back.)
 *
 * Use {@link RegionServerJMXPoolFactory} to reference these.
 */
public class RegionServerJMXInfo implements Closeable {
  private final KeyedObjectPool<String, RegionServerJMXHostInfo> pool;

  public RegionServerJMXInfo() {
    pool = RegionServerJMXPoolFactory.getInstance();
  }

  public double get75thPercentileReadLatency(final String hostName) {
    RegionServerJMXHostInfo hostInfo = null;
    try {
      hostInfo = pool.borrowObject(hostName);
      return hostInfo.get75thPercentileReadLatency();
    } catch (Exception e) {
      try {
        pool.invalidateObject(hostName, hostInfo);
      } catch (Exception e1) {
        throw Throwables.propagate(e1);
      }
      hostInfo = null;
      throw Throwables.propagate(e);
    } finally {
      if (hostInfo != null) {
        try {
          pool.returnObject(hostName, hostInfo);
        } catch (Exception e) {
        }
      }
    }
  }

  public int getCompactionQueueSize(final String hostName) {
    RegionServerJMXHostInfo hostInfo = null;
    try {
        hostInfo = pool.borrowObject(hostName);
      return hostInfo.getCompactionQueueSize();
    } catch (Exception e) {
      try {
        pool.invalidateObject(hostName, hostInfo);
      } catch (Exception e1) {
        throw Throwables.propagate(e1);
      }
      hostInfo = null;
      throw Throwables.propagate(e);
    } finally {
      if (hostInfo != null) {
        try {
          pool.returnObject(hostName, hostInfo);
        } catch (Exception e) {
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    try {
      pool.close();
    } catch (Exception e) {
      Throwables.propagate(e);
    }
  }
}
