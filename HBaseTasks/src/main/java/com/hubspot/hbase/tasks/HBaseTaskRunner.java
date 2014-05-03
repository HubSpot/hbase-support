
package com.hubspot.hbase.tasks;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.hubspot.hbase.tasks.config.HBaseTasksModule;
import com.hubspot.hbase.tasks.config.commandargs.CommandLineArgModule;
import com.hubspot.hbase.tasks.config.commandargs.IgnoreOptionParser;
import com.hubspot.hbase.tasks.jobs.ClusterStatusReporterJob;
import com.hubspot.hbase.tasks.jobs.CompactRegionsFromFileJob;
import com.hubspot.hbase.tasks.jobs.CompactionJob;
import com.hubspot.hbase.tasks.jobs.DisplayRegionStatsJob;
import com.hubspot.hbase.tasks.jobs.FlushMemstoreJob;
import com.hubspot.hbase.tasks.jobs.GracefulLoadJob;
import com.hubspot.hbase.tasks.jobs.GracefulShutdownJob;
import com.hubspot.hbase.tasks.jobs.HBaseBalancerJob;
import com.hubspot.hbase.tasks.jobs.MajorCompactServersJob;
import com.hubspot.hbase.tasks.jobs.MergeTableRegionsJob;
import com.hubspot.hbase.tasks.workers.HBaseBalancerWorker;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.util.List;

public class HBaseTaskRunner {
  private List<Task> tasks = Lists.newArrayList();


  public static void main(String[] args) {
    new HBaseTaskRunner().run(args);
  }

  private void run(String[] args) {
    registerJobs();

    Class<? extends Runnable> runnable = pickTask(args);
    Injector injector = Guice.createInjector(new CommandLineArgModule(args), new HBaseTasksModule());

    injector.getInstance(runnable).run();
  }

  private Class<? extends Runnable> pickTask(String[] args) {
    Options options = new Options();
    for (Task task : tasks) {
      options.addOption(task.shortOpt, task.longOpt, false, task.description);
    }

    try {
      CommandLine commandLine = new IgnoreOptionParser().parse(options, args);
      for (Task task : tasks) {
        if (commandLine.hasOption(task.shortOpt) || commandLine.hasOption(task.longOpt)) {
          return task.taskClass;
        }
      }
    } catch (ParseException e) {
      throw Throwables.propagate(e);
    }

    new HelpFormatter().printHelp(140, "hadoop jar {jar} {job} [commands]", "Job Choices", options, "", true);
    Options argOptions = new Options();

    CommandLineArgModule.addArgumentsToOptions(argOptions);
    new HelpFormatter().printHelp(140, "\n\n", "Job Options", argOptions, " ", true);
    System.exit(1);
    return null;
  }

  private void registerJobs() {
    register(CompactionJob.SHORT_OPT, CompactionJob.LONG_OPT, CompactionJob.DESCRIPTION, CompactionJob.class);
    register(DisplayRegionStatsJob.SHORT_OPT, DisplayRegionStatsJob.LONG_OPT, DisplayRegionStatsJob.DESCRIPTION, DisplayRegionStatsJob.class);
    register(HBaseBalancerJob.SHORT_OPT, HBaseBalancerJob.LONG_OPT, HBaseBalancerJob.DESCRIPTION, HBaseBalancerJob.class);
    register(GracefulShutdownJob.SHORT_OPT, GracefulShutdownJob.LONG_OPT, GracefulShutdownJob.DESCRIPTION, GracefulShutdownJob.class);
    register(GracefulLoadJob.SHORT_OPT, GracefulLoadJob.LONG_OPT, GracefulLoadJob.DESCRIPTION, GracefulLoadJob.class);
    register(MergeTableRegionsJob.SHORT_OPT, MergeTableRegionsJob.LONG_OPT, MergeTableRegionsJob.DESCRIPTION, MergeTableRegionsJob.class);
    register(MajorCompactServersJob.SHORT_OPT, MajorCompactServersJob.LONG_OPT, MajorCompactServersJob.DESCRIPTION, MajorCompactServersJob.class);
    register(FlushMemstoreJob.SHORT_OPT, FlushMemstoreJob.LONG_OPT, FlushMemstoreJob.DESCRIPTION, FlushMemstoreJob.class);
    register(CompactRegionsFromFileJob.SHORT_OPT, CompactRegionsFromFileJob.LONG_OPT, CompactRegionsFromFileJob.DESCRIPTION, CompactRegionsFromFileJob.class);
    register(HBaseBalancerJob.SHORT_OPT, HBaseBalancerJob.LONG_OPT, HBaseBalancerJob.DESCRIPTION, HBaseBalancerWorker.class);
    register(ClusterStatusReporterJob.JOB_NAME, ClusterStatusReporterJob.JOB_NAME, ClusterStatusReporterJob.DESCRIPTION, ClusterStatusReporterJob.class);
  }

  private void register(String shortOpt, String longOpt, String description, Class<? extends Runnable> taskClass) {
    tasks.add(new Task(shortOpt, longOpt, description, taskClass));
  }

  private static class Task {
    private final String shortOpt;
    private final String longOpt;
    private final String description;
    private final Class<? extends Runnable> taskClass;

    private Task(String shortOpt, String longOpt, String description, Class<? extends Runnable> taskClass) {
      this.shortOpt = shortOpt;
      this.longOpt = longOpt;
      this.description = description;
      this.taskClass = taskClass;
    }
  }
}
