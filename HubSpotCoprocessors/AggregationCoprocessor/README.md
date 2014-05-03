# HubSpot AggregationCoprocessor

A coprocessor for hbase to make aggregation easier. Inspired by http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/coprocessor/AggregationClient.html.

## Synopsis

### Usage:

```java
Map<byte[], Long> rowCounts = HubSpotAggregations.groupKeyCount(table, scan, new byte[]{(byte)0xFF, (byte)0xFF, 0, (byte)0xFF});

for (Map.Entry<byte[], Long> entry : rowCounts.entrySet()) {
   MyRowKey rowKey = rowKey.parseFrom(entry.getKey());
   System.out.println("Year - " + rowKey.getYear() + " had " + entry.getValue() + " rows");
}
```

### HBase Configuration

Add the coprocessor to your hbase configuration:

```java
TableRegistry.newTableBuilder()
    .addFamilyWithOneVersion(CF_LOG)
    .withGeneratedSplits(16, 50)
    .setValue("COPROCESSOR$1", getCoprocessorPath("HubSpotAggregationCoprocessor-1.0.1.jar") + "|" + HsAggProtocolImpl.class.getCanonicalName() + "|" + Coprocessor.PRIORITY_USER + "|")
    .register(HTABLE_CAMPAIGN_ROLLUPS);

private static String getCoprocessorPath(final String jarName) {
  if (ZkUtils.getDefaultZkConfig().getEnvironment().isDeployed()) {
    return "hdfs:///coprocessors/" + jarName;
  } else {
    return new File(getCoprocessorDirectory(), jarName).getAbsolutePath();
  }
}

private static File getCoprocessorDirectory() {
  return new File(HBaseSchema.class.getProtectionDomain().getCodeSource().getLocation().getPath(), "../../../coprocessors");
}
```
