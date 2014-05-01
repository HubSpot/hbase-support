package com.hubspot.hbase.tasks.serialization;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;

public class WritableDeserializer<T extends Writable> extends JsonDeserializer<T> {
  @Override
  @SuppressWarnings("unchecked")
  public T deserialize(final JsonParser jp, final DeserializationContext ctxt) throws IOException, JsonProcessingException {
    Class<T> clazz = (Class<T>)((ParameterizedType)getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    try {
      final Writable result = clazz.newInstance();
      final byte[] bytes = Base64.decodeBase64(jp.getValueAsString());
      result.readFields(ByteStreams.newDataInput(bytes));
      return (T)result;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
