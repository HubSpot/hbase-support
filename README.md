# hbase-support

Supporting configs and tools for HBase at HubSpot

We currently have three projects to share, but have more to add!

## HBaseTasks

A collection of tasks that can be used to augment the standard jruby scripts + hbase master. See [HBaseTasks](./tree/master/HBaseTasks/).

## hystrix

We currently use a modified hbase client with [hystrix](https://github.com/Netflix/Hystrix) to prevent one single region server slowness from causing upstream problems. See [the patch](./tree/master/hystrix/hystrix.diff), and a [github comparison](https://github.com/HubSpot/hbase/compare/cdh4-0.94.2_4.2.0...cdh-4.2.0-hubspot).

## HubSpotCoprocessors

A collections of coprocessors we use with maven configuration to scope each one's classpath by version. It includes a HyperLogLog coprocessor, a count group by coprocessor, and a count-min sketch coprocessor. See [HubSpotCoprocessors](./tree/master/HubSpotCoprocessors).
