package com.hubspot.hbase.tasks.serialization;


import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hbase.ServerName;

import java.io.IOException;

public class ServerNameSerialization {
  public static class ServerNameDeserializer extends JsonDeserializer<ServerName> {
    @Override
    public ServerName deserialize(final JsonParser jp, final DeserializationContext ctxt) throws IOException, JsonProcessingException {
      return ServerName.parseVersionedServerName(Base64.decodeBase64(jp.getValueAsString()));
    }
  }

  public static class ServerNameSerializer extends JsonSerializer<ServerName> {
    @Override
    public void serialize(final ServerName value, final JsonGenerator jgen, final SerializerProvider provider) throws IOException, JsonProcessingException {
      jgen.writeString(Base64.encodeBase64String(value.getVersionedBytes()));
    }
  }
}
