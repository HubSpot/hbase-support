package com.hubspot.hbase.tasks.config;

import com.google.common.base.Throwables;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.hubspot.hbase.tasks.balancing.Balancer;
import com.hubspot.hbase.tasks.balancing.annealing.AnnealingBalancer;
import com.hubspot.hbase.tasks.balancing.config.OptimizationModule;
import com.hubspot.liveconfig.LiveConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseAdminWrapper;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

public class HBaseTasksModule extends AbstractModule {
  public static final String ACTIVE_BALANCER = "active balancer";
  public static final String HBASE_PATH = "hbase path";

  @Override
  protected void configure() {
    install(new SetupLiveConfigModule());
    install(new OptimizationModule());
    bind(Balancer.class).annotatedWith(Names.named(ACTIVE_BALANCER)).to(AnnealingBalancer.class);
  }

  @Provides
  @Singleton
  // TODO - maxiak - maybe a better configuration retriever here?
  public Configuration providesConfiguration() {
    return HBaseConfiguration.create();
  }

  @Provides
  @Singleton
  public HBaseAdminWrapper providesHBaseAdminWrapper(Configuration configuration) {
    try {
      return new HBaseAdminWrapper(new HBaseAdmin(configuration));
    } catch (MasterNotRunningException e) {
      throw Throwables.propagate(e);
    } catch (ZooKeeperConnectionException e) {
      throw Throwables.propagate(e);
    }
  }

  @Provides
  @Singleton
  public FileSystem providesHadoopFilesystem(Configuration conf) {
    try {
      return FileSystem.get(conf);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  // Override with any of the number of ways to override zkConfig. E.g. HBASE_TASKS_ROOT_HDFS_PATH in env.
  @Provides
  @Named(HBASE_PATH)
  public Path providesHBasePath(LiveConfig liveConfig) {
    return new Path(liveConfig.getStringMaybe("hbase.tasks.root.hdfs.path").or("/hbase"));
  }
}
