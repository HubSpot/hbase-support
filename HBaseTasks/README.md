# HBaseTasks

HBaseTasks is a collection of java utilities to augment the standard hbase master actions. The java project hooks into HBaseAdmin and performs utilities such as custom balancing, status reporting, and compaction behaviors. These tasks have been extracted out of tasks that have been running on production in HubSpot for months.

## Quick Start
The initial release is compiled against **HBase 0.94.15-cdh4.6.0** (as determined by the `hbase.version` property in the pom). It's possible that this project will work against a close version, but use at your own risk. 

To start, let's just get some stats of our region locality (% of data that's local to the region server). Download [HBaseTasks-0.1-SNAPSHOT-jar-with-dependencies.jar](https://www.dropbox.com/s/64r9lzi582ztcr7/HubSpotHBaseTasks-0.1-SNAPSHOT-jar-with-dependencies.jar) and place on a server configured for your cluster (e.g. a standby master). Then run:

```bash
$ hadoop jar HBaseTasks-0.1-SNAPSHOT-job.jar -displayRegionStats
```

The command will then output something like this:

```
+---------------------------------------------------------------------------------------------------+
|                                       Region info by table                                        |
+---------------------------------------------------------------------------------------------------+
|Table name                                        |Count  |Load    |Size   |50th   |95th   |Lcty   |
|--------------------------------------------------+-------+--------+-------+-------+-------+-------|
|tableA                                            |301    |0.001   |164 MB |53 KB  |2 MB   |1.0000 |
|tableB                                            |201    |0.001   |229 MB |1 MB   |1 MB   |1.0000 |
+---------------------------------------------------------------------------------------------------+



+---------------------------------------------------------------------------------------------------+
|                                       Region info by server                                       |
+---------------------------------------------------------------------------------------------------+
|Region server                                     |Count  |Load    |Size   |50th   |95th   |Lcty   |
|--------------------------------------------------+-------+--------+-------+-------+-------+-------|
|server2.domain.net                                |1243   |0.467   |101 GB |32 KB  |39 MB  |1.0000 |
|server1.domain.net                                |1327   |0.533   |100 GB |33 KB  |39 MB  |1.0000 |
+---------------------------------------------------------------------------------------------------+
```

At HubSpot, we rarely see localities below 1.0. Part of that reason is the set of utilities in this package.

Let's try running the balancer (without `actuallyRun`):

```
Ran 3840 steps
---------------------------------------------
  Cost for 'Initial'
---------------------------------------------
   DataSizeCost          x 1000.00 -> 537.227
   ProximateRegionKeyCost x 5.00 -> 4.576
   LocalityCost          x 75.00 -> 0.000
   TransitionCost        x 2000.00 -> 0.000
---------------------------------------------
     TOTAL: 541.803
---------------------------------------------


---------------------------------------------
  Cost for 'Final'
---------------------------------------------
   DataSizeCost          x 1000.00 -> 7.545
   ProximateRegionKeyCost x 5.00 -> 4.103
   LocalityCost          x 75.00 -> 0.000
   TransitionCost        x 2000.00 -> 0.884
---------------------------------------------
     TOTAL: 12.532
---------------------------------------------


Total transitions: 502 / 750
```

We see that it thinks that if we move 502 regions (out of the 750 allowed on this run), then we will
have a significant improvement.


## Help

If you run the command `hadoop jar HBaseTasks-0.1-SNAPHSOT-job.jar` without any arguments, it will print the help. At the time of writing, that is:

```
usage: hadoop jar {jar} {job} [commands] [-clusterStatusReporter] [-compact] [-compactRegionsFromFile] [-displayRegionStats]
       [-flushMemstore] [-gracefulLoad] [-gracefulShutdown] [-majorCompactServers] [-majorCompactTable] [-mergeRegionsInTable]
       [-runCustomBalancer]
Job Choices
 -clusterStatusReporter,--clusterStatusReporter     Daemon which periodicially reports cluster status, including region-level stats, to
                                                    OpenTSDB
 -compact,--compact                                 Requests compaction for every enabled table, or table specified (-table). Major compact
                                                    is default, specify -compactionType minor to minor compact
 -compactRegionsFromFile,--compactRegionsFromFile   Compact regions specified in a file, if inputFile is specified.  If outputFile is
                                                    specified, dump all regions to a file.
 -displayRegionStats,--displayRegionStats           Get and show region stats for the cluster.
 -flushMemstore,--flushMemstore                     Flush memstore for the table specified
 -gracefulLoad,--gracefulLoad                       Gracefully load regions onto a region server.
 -gracefulShutdown,--gracefulShutdown               Gracefully shutdown the region server.
 -majorCompactServers,--majorCompactServers         Major compact all regions on a server or list of servers
 -mergeRegionsInTable,--mergeRegionsInTable         DANGER! Merges regions in a disabled table.  Must major compact table prior to running.
 -runCustomBalancer,--runCustomBalancer             Run the custom balancer.
usage:
       [-actuallyRun <arg>] [-allRegions <arg>] [-compactionQueueSize <arg>] [-compactionQuietTimeSeconds <arg>] [-compactionType <arg>]
       [-compactPostMove <arg>] [-compactThreshold <arg>] [-costFunction <arg>] [-emptyRegionsOnly <arg>] [-factor <arg>] [-inputFileName
       <arg>] [-maxTimeMillis <arg>] [-maxTransitions <arg>] [-maxTransitionsPerMinute <arg>] [-optimizationTimeSeconds <arg>]
       [-outputFileName <arg>] [-perturber <arg>] [-pullMostLocal <arg>] [-reverseLocality <arg>] [-serverName <arg>] [-source <arg>]
       [-stagnateIterations <arg>] [-table <arg>]
Job Options
 -actuallyRun,--actuallyRun <arg>                                 JOBS: runCustomBalancer, mergeTableRegions. If true, then actually apply
                                                                  the result of the balancer to the cluster. For mergeTableRegions, actually
                                                                  execute the merges [default: false]
 -compactionQueueSize,--compactionQueueSize <arg>                 JOB: majorCompactTable. Amount of simultaneous compactions each servers
                                                                  should target.
 -compactionQuietTimeSeconds,--compactionQuietTimeSeconds <arg>   JOB: majorCompactTable. Amount of seconds to wait between compactions.
                                                                  [Default: 5]
 -compactionType,--compactionType <arg>                           Type of compaction to run: minor or major
 -compactPostMove,--compactPostMove <arg>                         If true, automatically compact a region after moving to another server,
                                                                  unless @compactThreshold is reached.
 -compactThreshold,--compactThreshold <arg>                       JOB: gracefulLoad, runCustomBalancer.  Will skip compacting a region if
                                                                  its locality is greater than this threshold.
 -costFunction,--costFunction <arg>                               JOB: runCustomBalancer. A JSON structure with keys of DataSizeCost,
                                                                  LoadCost, LocalityCost, RegionCountCost, TransitionCost,
                                                                  ProximateRegionKeyCost and values of relative weights.
 -emptyRegionsOnly,--emptyRegionsOnly <arg>                       JOB: mergeRegionsInTable. If true, merger will only merge empty regions
                                                                  with a nearby non-empty region
 -factor,--regionMergeFactor <arg>                                The number of regions to combine into 1.  I.E. if you specify 2, number of
                                                                  regions will be cut in half.  5 would reduce by a factor of 5.
 -inputFileName,--inputFileName <arg>                             JOB: runCustomBalancer. If provided, use file for getting information
                                                                  rather than calling hbase.
 -maxTimeMillis,--maxTimeMillis <arg>                             The number of milliseconds for which the operation should go on for.
                                                                  [Default: 600000]
 -maxTransitions,--maxTransitions <arg>                           JOBS: runCustomBalancer,gracefulShutdown. The maximum number of
                                                                  simultaneous transitions the master should be performing at any one time.
                                                                  [Default: 5]
 -maxTransitionsPerMinute,--maxTransitionsPerMinute <arg>         JOB: runCustomBalancer. The max number of transitions we should try to
                                                                  attempt in a minute. [Default: 75]
 -optimizationTimeSeconds,--optimizationTimeSeconds <arg>         JOB: runCustomBalancer. The max number of seconds we can spend optimizing
                                                                  regions. [Default: 30]
 -outputFileName,--outputFileName <arg>                           JOB: runCustomBalancer. If provided, export balancing data to file for
                                                                  offline processing.
 -perturber,--perturber <arg>                                     JOB: runCustomBalancer. Which perturber to apply to try to optimize. One
                                                                  of CombinedPerturber, SomewhatGreedyPerturber, GreedyPerturber,
                                                                  TableGreedyPerturber or RandomPerturber
 -pullMostLocal,--pullMostLocal <arg>                             JOB: gracefulLoad. When true, will load the target server with the regions
                                                                  most local to it.  Great for bringing up a RS that died. [Default: false]
 -reverseLocality,--reverseLocality <arg>                         JOB: majorCompactTable. If true, then compact in reverse locality order.
 -serverName,--serverName <arg>                                   JOBS: gracefulShutdown, gracefulLoad. Specifies which server to gracefully
                                                                  shutdown or load.
 -source,--sourceServerName <arg>                                 JOB: gracefulLoad. Specifies which server to pull regions FROM (I.E. drain
                                                                  regions from one server into another).
 -stagnateIterations,--stagnateIterations <arg>                   JOB: runCustomBalancer. The number of iterations to run while not getting
                                                                  any improvement before giving up. [DEFAULT: 100]
 -table,--table <arg>                                             The name of the table to operate on.
```

This may seem like a daunting set of options, but in practice you'll only ever need to specify at most 3-5 options at once. The key here is that the help is split into **two** groups -- one list of options for the type of job being run, and the other for the general list of options that's shared throughout. The code is written such that options get passed through to places they're needed automatically. For example, if the gracefulLoad job uses the custom compation logic, then the compaction arguments will work on that job.

## Locality

Locality is mentioned quite a lot in this project -- for good reason. We've discovered emperically that maintaining high locality is one of the easiest ways to reduce variance in performance and cascading failures. Unless otherwise noted, we define locality as **the percentage of data "owned" by the region server that is not on the region server itself**. By owned, we mean that this does not take into consideration what data is being read, only the total data on the regions assigned to the region server. In our use cases, the data access pattern is too random to allow blocks of our data to stay non local for long.

As an example, suppose region server A serves 5 regions, each having only one hfile of size 1GB. The hdfs block locations tell us:

- region 1's hfile has 100MB on server A
- region 2's hfile has 200MB on server A
- region 3's hfile has 300MB on server A
- region 4's hfile has 400MB on server A
- region 5's hfile has 500MB on server A

The locality of server A, would be:

```
100MB + 200MB + 300MB + 400MB + 500MB / (1GB x 5) = 30%
```


## Tasks

### displayRegionStats

You already saw this in the Quick Start. It simply displays information about your region sizes split by table and by server, then provides hdfs block locality information for each slide of data.

#### Options

- -outputFileName : If specified, this exports all of the region data to a compressed json file. This can then be used by the custom balancer to "dry run" the optimizer and see what transitions the balancer would perform on a remote cluster.

### compact

This program compacts regions on the cluster. The program manages the queue and will slowly trickle out compcation requests on a per server basis. In this way, if you SIGINT this program, it will stop compacting and the region servers will only have one compaction to finish. Moreover, you can pass the `maxTimeMillis` argument to specify how long you want compaction to last. This is useful if you have a few hours for which you can run compactions and you want to utilize those few hours as much as possible.

#### Options

- -table TABLE NAME : If specified, limit the regions to compact to this table. Otherwise, look at all tables.
- -maxTimeMillis MILLIS - The total amount of the the script should be compacting.
- -reverseLocality : If true, compact the regions with the least locality first. By default it compacts regions with more hfiles first.
- -compactionQueueSize QUEUE_SIZE : How large the queue size on the region server can be allowed to be. Once the queue is this large the compaction job waits to submit.
- -compactionQuietTimeSeconds SECONDS : After the region server has room (deterined by compactionQueueSize), how long do we wait until the region server gets another region to compact.
- -compactionType TYPE : Either "major" or "minor". Defaults to "major".
- -compactThreshold LOCALITY : If the region has grater than LOCALITY, don't bother compacting it. This is useful if you only want to compact badly distributed regions to regain locality. Default: MAX_VALUE

### compactRegionsFromFile

If you are performing another job or you have other reasons to compact. This job can be used to major compact all regions in a file.

You can run this with either the inputFile or the outputFile argument depending on if you want to dump reginos to a file or actually compact from a file.

#### Options

- -inputFile FILE_PATH : The input file of regions to compact.
- -outputFile OUTPUT_FILE : The output file to dump region names.

### flushMemstore

This job is used to synchronously flush the memstore in a table. 

#### Options

- -table TABLE : The table to flush.

### gracefulLoad

Load regions onto a server, in such a way that it's balanced
with the rest of the cluster.

#### Options


##### Regions to load

- -source SERVER_HOST_NAME : Instead of pulling from the cluster, you can take from another server.
- -pullMostLocal : If specified, try to pull regions that have the highest locality on the 
- -inputFile FILE_PATH : If provided, load regions that are listed in the file.

##### General options

- -serverName SERVER_HOST_NAME : The server host name according to hbase target server.
- -compactPostMove : If specified, compact regions as they're moved to the destination server.

##### Compaction options (if compact post move)

- -compactionQueueSize QUEUE_SIZE : How large the queue size on the region server can be allowed to be. Once the queue is this large the compaction job waits to submit.
- -compactionQuietTimeSeconds SECONDS : After the region server has room (deterined by compactionQueueSize), how long do we wait until the region server gets another region to compact.
- -compactionType TYPE : Either "major" or "minor". Defaults to "major".
- -compactThreshold LOCALITY : If the region has grater than LOCALITY, don't bother compacting it. This is useful if you only want to compact badly distributed regions to regain locality. Default: MAX_VALUE

### gracefulShutdown

Shutdown a server gracefully, by unloading regions.

#### Options

- -serverName SERVER_HOST_NAME : The server host name according to hbase target server.
- -compactPostMove : If specified, compact regions as they're moved to the destination server.
- -outputFile FILE_PATH : Write new region assignments to this file.

##### Compaction options (if compact post move)

- -compactionQueueSize QUEUE_SIZE : How large the queue size on the region server can be allowed to be. Once the queue is this large the compaction job waits to submit.
- -compactionQuietTimeSeconds SECONDS : After the region server has room (deterined by compactionQueueSize), how long do we wait until the region server gets another region to compact.
- -compactionType TYPE : Either "major" or "minor". Defaults to "major".
- -compactThreshold LOCALITY : If the region has grater than LOCALITY, don't bother compacting it. This is useful if you only want to compact badly distributed regions to regain locality. Default: MAX_VALUE

### runCustomBalancer

This runs a simulated annealing based balancer on the cluster after telling the HBaseAdmin to stop running its own balancer.

#### Options

##### Region moving options

- -actuallyRun : If specified, actually apply the transitions. Otherwise, just display what transitions it would make and what impact it would have on the cluster.
- -inputFile FILE_PATH : A path to the file exported by `displayRegionStats -outputFile`. If specified, use this file to optimize rather than the actual cluster. This is useful to test what the balancer would do without running the balancer on a cluster.
- -maxOptimizationTimeSeconds SECONDS : Limit how long the optimization step can last. Default: 30 seconds.
- -maxTransitionsPerMinute TRANSITIONS : Limits the number of region moves on the cluster per minute. Default: 75
- -table TABLE_NAME : Limit the balancing to just this one table.
- -maxTimeMillis MILLISECONDS : Approximately how long the balancing step should take.


##### Simulated annealing options

- -costFunction COST_FUNCTION_JSON : A JSON object representing the relative weights of various cost functions: "DataSizeCost, LoadCost, LocalityCost, RegionCountCost, TransitionCost, ProximateRegionKeyCost". Default: `{"DataSizeCost":1000,"ProximateRegionKeyCost":5,"LocalityCost":75,"TransitionCost":2000}`
- -perturber PERTURBER : The name of the perturber used to move regions around. One of CombinedPerturber, "SomewhatGreedyPerturber, GreedyPerturber, TableGreedyPerturber or RandomPerturber". Default: `TableGreedyPerturber`
- -stagnateIterations ITERATIONS : The number of iterations the simulated annealing is allowed to "stagnate" before the optimization is short circuited.

### mergeRegionsInTable

Allows merging of regions for a disabled table, in an otherwise online HBase cluster.

DANGER! We have used this to merge the regions of multiple production tables, but it is provided AS-IS.  I recommend familiarizing yourself with the code——which was taken from HBase's own source and made publicly accessible——and testing on QA or test tables.

Here was our workflow, to make this as safe as possible:

1. Disable target table
2. Snapshot table.
3. Create clone from snapshot: ```clone-table-name```.
4. Major compact ```clone-table-name``` (important! make sure this finishes before continuing).
5. Verify 3 finished.  You can ```hdfs dfs -ls -R /hbase/clone-table-name```, and see that there are no pointer files from the initial snapshot.
6. Disable ```clone-table-name```
7. Run mergeRegionsInTable on ```clone-table-name```
8. Enable ```clone-table-name```
9. Do some scans, run hbase hbck, or otherwise verify it looks good.
10. If good, use this as the new table.  Otherwise, drop table and use the original (still disabled) table.

The clone -> compact workflow provides you some safety incase something goes wrong.

##### Options

- -actuallyRun : If specified, actually run the merge.  Otherwise just goes through the motions and prints some debug output
- -table TABLE_NAME : the table to merge regions for
- -factor NUM : By what facter should reduce the number of regions.  You will be left with approximately ```currentRegions / NUM``` regions after.  I.E. Factor of 2 would cut the number in half.  Ignored if ```-emptyRegionsOnly``` is specified.
- -emptyRegionsOnly : If specified, finds all sets of contiguous empty regions and merges them with the nearest non-empty region for each set.  Ignores ```-factor``` options when set.
- 
## License
HBaseTasks is released under the Apache 2.0 License.

## Authors
This project was written by [HubSpot deelopers](http://dev.hubspot.com/).

