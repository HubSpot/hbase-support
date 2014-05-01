package com.hubspot.hbase.tasks.serialization;

import com.fasterxml.jackson.databind.module.SimpleModule;
import com.hubspot.hbase.tasks.serialization.ServerNameSerialization.ServerNameDeserializer;
import com.hubspot.hbase.tasks.serialization.ServerNameSerialization.ServerNameSerializer;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

public class SerializationModule {
  private SerializationModule() {
  }

  public static void fixJacksonModule() {
    final SimpleModule module = new SimpleModule();
    module.addDeserializer(ServerName.class, new ServerNameDeserializer());
    module.addSerializer(ServerName.class, new ServerNameSerializer());
    module.addDeserializer(HRegionInfo.class, new WritableDeserializer<HRegionInfo>() {
    });
    module.addSerializer(HRegionInfo.class, new WritableSerializer<HRegionInfo>() {
    });
    ObjectMapperSingleton.MAPPER.registerModule(module);
  }
}
