package com.hubspot.hbase.tasks.serialization;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;

import java.util.Collection;
import java.util.Collections;

public enum ObjectMapperFactory {
  INSTANCE;

  public ObjectMapper build() {
    return build(Collections.<Module>emptyList());
  }

  public ObjectMapper build(Collection<? extends Module> extraModules) {
    final ObjectMapper mapper = new ObjectMapper();

    mapper.registerModule(new GuavaModule());

    for (Module module : extraModules) {
      mapper.registerModule(module);
    }

    mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, false);
    mapper.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, true);
    mapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, true);
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);

    return mapper;
  }
}
