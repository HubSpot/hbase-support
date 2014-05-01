package com.hubspot.hbase.tasks.helpers.jmx;

import com.google.common.base.Throwables;
import com.google.common.io.Closeables;

import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.Closeable;
import java.io.IOException;

/**
 * A wrapper around
 */
public class RegionServerJMXHostInfo implements Closeable {
  private static final int DEFAULT_JMX_PORT = 10102;
  private MBeanServerConnection mbsc;
  private JMXConnector jmx;

  public RegionServerJMXHostInfo(final String hostname) {
    try {
      final JMXServiceURL url = new JMXServiceURL(String.format("service:jmx:rmi:///jndi/rmi://%s:%s/jmxrmi", hostname, DEFAULT_JMX_PORT));
      jmx = JMXConnectorFactory.connect(url, null);
      mbsc = jmx.getMBeanServerConnection();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public int getCompactionQueueSize() {
    ObjectName regionServerStats = queryForFirst(mbsc, "hadoop:name=RegionServerStatistics,service=RegionServer");
    try {
      return Integer.parseInt(mbsc.getAttribute(regionServerStats, "compactionQueueSize").toString());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public double get75thPercentileReadLatency() {
    ObjectName regionServerStats = queryForFirst(mbsc, "hadoop:name=RegionServerStatistics,service=RegionServer");
    try {
      return Double.parseDouble(mbsc.getAttribute(regionServerStats, "fsReadLatencyHistogram_75th_percentile").toString());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private ObjectName queryForFirst(MBeanServerConnection mbsc, String prefix) {
    try {
      return mbsc.queryNames(getObjectName(prefix), null).iterator().next();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private ObjectName getObjectName(String name) {
    try {
      return new ObjectName(name);
    } catch (Exception e) {
      throw com.google.common.base.Throwables.propagate(e);
    }
  }

  boolean isAlive() {
    try {
      // Any reasonable JVM with JMX will expose this.
      mbsc.getAttribute(new ObjectName("java.lang:type=Memory"), "HeapMemoryUsage");
    } catch (MalformedObjectNameException e) {
      throw Throwables.propagate(e);
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  @Override
  public void close() throws IOException {
    if (jmx != null) {
      Closeables.closeQuietly(jmx);
    }
  }
}
