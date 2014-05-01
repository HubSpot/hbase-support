package com.hubspot.hbase.tasks.config.commandargs;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.util.Map;


/**
 * A module for passing command line arguments around.
 */
public class CommandLineArgModule extends AbstractModule {
  private final String[] args;

  public CommandLineArgModule(String[] args) {
    this.args = args;
  }

  @Override
  protected void configure() {
    CommandLine commandLine = parseCommandLine(args);

    for (HBaseTaskOption taskOption : HBaseTaskOption.values()) {
      bindValue(taskOption.getTargetClass(), taskOption, commandLine);
    }
  }

  @SuppressWarnings("unchecked")
  private <T> void bindValue(Class<T> clazz, HBaseTaskOption taskOption, CommandLine commandLine) {
    Optional<T> value = Optional.of((T)getValue(commandLine, taskOption));
    bind(typeLiterals.get(taskOption.getTargetClass())).annotatedWith(ForArgs.forArg(taskOption)).toInstance(value);
  }

  private Object getValue(CommandLine commandLine, HBaseTaskOption taskOption) {
    if (Boolean.class.equals(taskOption.getTargetClass())) {
      return commandLine.hasOption(taskOption.getLongName());
    } else if (commandLine.getOptionValue(taskOption.getLongName()) != null) {
      return CommandLineParsers.PARSERS.get(taskOption.getTargetClass()).apply(commandLine.getOptionValue(taskOption.getLongName()));
    } else {
      return taskOption.getDefaultValue();
    }
  }

  private CommandLine parseCommandLine(String[] args) {
    Options options = new Options();

    for (HBaseTaskOption taskOption : HBaseTaskOption.values()) {
      options.addOption(taskOption.getShortName(), taskOption.getLongName(), true, taskOption.getDescription());
    }

    try {
      return new IgnoreOptionParser().parse(options, args);
    } catch (ParseException e) {
      throw Throwables.propagate(e);
    }
  }

  private static final Map<Class, TypeLiteral> typeLiterals = ImmutableMap.<Class, TypeLiteral>builder()
          .put(Integer.class, new TypeLiteral<Optional<Integer>>(){})
          .put(Double.class, new TypeLiteral<Optional<Double>>(){})
          .put(String.class, new TypeLiteral<Optional<String>>(){})
          .put(Boolean.class, new TypeLiteral<Optional<Boolean>>(){})
          .put(Long.class, new TypeLiteral<Optional<Long>>(){})
          .put(Short.class, new TypeLiteral<Optional<Short>>(){})
          .put(Float.class, new TypeLiteral<Optional<Float>>(){})
          .build();
}
