# hbase-support

Supporting configs and tools for HBase at HubSpot

We currently have three projects to share, but have more to add!

## HBaseTasks

A collection of tasks that can be used to augment the standard jruby scripts + hbase master. See [HBaseTasks](https://github.com/HubSpot/hbase-support/tree/master/HBaseTasks/).

## hystrix

We currently use a modified hbase client with [hystrix](https://github.com/Netflix/Hystrix) to prevent one single region server slowness from causing upstream problems. See our [hystrix](https://github.com/HubSpot/hbase-support/tree/master/hystrix/) repository for more information.

## HubSpotCoprocessors

A collections of coprocessors we use with maven configuration to scope each one's classpath by version. It includes a HyperLogLog coprocessor, a count group by coprocessor, and a count-min sketch coprocessor. See [HubSpotCoprocessors](https://github.com/HubSpot/hbase-support/tree/master/HubSpotCoprocessors).
