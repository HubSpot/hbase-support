package com.hubspot.hbase.tasks.serialization;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.IOException;

public class WritableSerializer<T extends Writable> extends JsonSerializer<T> {
  @Override
  public void serialize(final T value, final JsonGenerator jgen, final SerializerProvider provider) throws IOException, JsonProcessingException {
    jgen.writeString(Base64.encodeBase64String(WritableUtils.toByteArray(value)));
  }
}
