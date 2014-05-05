# Hystrix in HBase

The default HBase client suffers when a region server starts to slow down. By using [Hystrix](), we can automatically reject requests to region servers when they start behaving poorly. This patch adds a lot of configuration options to the HBase configuration object.

See the [patch](https://github.com/HubSpot/hbase-support/blob/master/hystrix/hystrix.diff) that we include in our client at HubSpot.

## Configuration options

### hbase.regionserver.enable.hystrix

Default: false

if true, enable hystrix. Otherwise, use the standard write path.

### hbase.regionserver.thread.core.size

Default: 10

The number of threads in the per-region-server thread pool.

### hbase.regionserver.thread.queue.rejection.threshold

Default -1

An artificial threshold in addition to the regular thread queue size that allows dynamic resizing since thread pools cannot handle dynamic resizing of queues.

### hbase.regionserver.thread.queue.size

Default: 10

The maximum size of the queue ahead of the region server thread pool before execution gets rejected. The default of -1 means that there is no queue, and if the thread pool is full executions get rejected.


### hbase.regionserver.circuit.breaker.error.threshold.percentage

Default: 25

The percent of errors that must occur in in the last 10 seconds for the circuit to open.

### hbase.regionserver.circuit.breaker.volume.threshold

Default: 10

The minimum amount of requests in 10 seconds before a circuit can open.

### hbase.regionserver.circuit.breaker.sleep.millis

Default: rpc timeout

How long after a circuit opens before hystrix should attempt to request again.

## Thread usage

Since we're pushing each region server request behind a threadpool, your client will use `{core size} x {number of region servers}` more threads than you're used to.

## License

```
Copyright 2014 HubSpot

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```