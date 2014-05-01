package com.hubspot.hbase.tasks.serialization;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Created by axiak on 01/05/2014.
 */
public class ObjectMapperSingleton {
  public static final ObjectMapper MAPPER = ObjectMapperFactory.INSTANCE.build();
  public final static JsonFactory JSON_FACTORY = MAPPER.getFactory();
}
