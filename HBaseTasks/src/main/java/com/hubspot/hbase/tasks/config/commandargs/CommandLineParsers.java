package com.hubspot.hbase.tasks.config.commandargs;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

class CommandLineParsers {
  private CommandLineParsers() {}

  private static final Function<String, Integer> TO_INTEGER = new Function<String, Integer>() {
    @Override
    public Integer apply(final String input) {
      return Integer.valueOf(input);
    }
  };

  private static final Function<String, Long> TO_LONG = new Function<String, Long>() {
    @Override
    public Long apply(final String input) {
      return Long.valueOf(input);
    }
  };

  private static final Function<String, Double> TO_DOUBLE = new Function<String, Double>() {
    @Override
    public Double apply(final String input) {
      return Double.valueOf(input);
    }
  };

  private static final Function<String, Float> TO_FLOAT = new Function<String, Float>() {
    @Override
    public Float apply(final String input) {
      return Float.valueOf(input);
    }
  };

  static final Map<Class<?>, Function<String, ?>> PARSERS = ImmutableMap.<Class<?>, Function<String, ?>>builder()
          .put(Integer.class, TO_INTEGER)
          .put(Long.class, TO_LONG)
          .put(Double.class, TO_DOUBLE)
          .put(Float.class, TO_FLOAT)
          .put(String.class, Functions.<String>identity())
          .build();

}
