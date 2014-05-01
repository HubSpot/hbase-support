package com.hubspot.hbase.tasks.balancing.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.google.common.base.Throwables;
import com.hubspot.hbase.tasks.balancing.annealing.AcceptanceFunction;
import com.hubspot.hbase.tasks.balancing.annealing.StandardAcceptanceFunction;
import com.hubspot.hbase.tasks.balancing.annealing.StandardTemperatureFunction;
import com.hubspot.hbase.tasks.balancing.annealing.TemperatureFunction;
import com.hubspot.hbase.tasks.balancing.annealing.perturbers.*;
import com.hubspot.hbase.tasks.balancing.cost.CostFunction;
import com.hubspot.hbase.tasks.balancing.cost.CostFunctionFactories;
import com.hubspot.hbase.tasks.balancing.cost.CostFunctionFactories.CostFunctionFactory;
import com.hubspot.hbase.tasks.balancing.cost.CostFunctions;
import com.hubspot.hbase.tasks.balancing.migration.MigrationPerturber;
import com.hubspot.hbase.tasks.config.commandargs.ForArg;
import com.hubspot.hbase.tasks.config.commandargs.HBaseTaskOption;
import com.hubspot.hbase.tasks.serialization.ObjectMapperSingleton;
import com.hubspot.liveconfig.value.Value;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class OptimizationModule extends AbstractModule {
  private static final Log LOG = LogFactory.getLog(OptimizationModule.class);

  public static final String COST_FUNCTION = "cost function";
  public static final String TEMP_FUNCTION = "default temp function";
  public static final String TOTAL_TRANSITIONS = "total transitions";

  private static final ImmutableMap<String, Class<? extends Perturber>> PERTURBERS = ImmutableMap.<String, Class<? extends Perturber>>builder()
      .put("RandomPerturber", RandomPerturber.class)
      .put("CombinedPerturber", CombinedPerturber.class)
      .put("SomewhatGreedyPerturber", SomewhatGreedyPerturber.class)
      .put("GreedyPerturber", GreedyPerturber.class)
      .put("TableGreedyPerturber", TableGreedyPerturber.class)
      .put("MigrationPerturber", MigrationPerturber.class)
      .build();

  @Override
  protected void configure() {
    bind(AcceptanceFunction.class)
        .annotatedWith(Names.named(COST_FUNCTION))
        .to(StandardAcceptanceFunction.class);
    bind(TemperatureFunction.class)
        .annotatedWith(Names.named(TEMP_FUNCTION))
        .toInstance(StandardTemperatureFunction.QUADRATIC_FUNCTION);
    bind(AcceptanceFunction.class)
        .to(StandardAcceptanceFunction.class);
  }

  @Singleton
  @Provides
  public Perturber providesPerturber(final Injector injector,
                                     @ForArg(HBaseTaskOption.PERTURBER)
                                     final Optional<String> perturberName) {
    return injector.getInstance(PERTURBERS.get(perturberName.or("TableGreedyPerturber")));
  }

  @Named(COST_FUNCTION)
  @Singleton
  @Provides
  public CostFunction providesCostFunction(final Injector injector,
                                           @Named(TOTAL_TRANSITIONS) int totalTransitions,
                                           @Named("hbase.tasks.balance.cost.function") Value<String> zkCostFunctionDefinition,
                                           @ForArg(HBaseTaskOption.COST_FUNCTION)
                                           Optional<String> costFunction) {
    final Map<String, Double> costFunctionArgs;
    try {
      costFunctionArgs = ObjectMapperSingleton.MAPPER.readValue(costFunction.or(zkCostFunctionDefinition.get()), new TypeReference<Map<String, Double>>(){});
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    final CostFunction[] costFunctions = new CostFunction[costFunctionArgs.size()];

    int i = 0;
    for (final Map.Entry<String, Double> entry : costFunctionArgs.entrySet()) {
      final CostFunctionFactory costFunctionFactory = CostFunctionFactories.getCostFunctionFactory(entry.getKey());
      if (costFunctionFactory == null) {
        throw new IllegalArgumentException(String.format("Invalid cost function name: '%s'", entry.getKey()));
      }

      CostFunction func = costFunctionFactory.build(entry.getValue(), totalTransitions);
      injector.injectMembers(func);
      costFunctions[i++] = func;
    }

    final CostFunction result = CostFunctions.combine(costFunctions);

    LOG.info(String.format("Cost Function: %s", result));
    return result;
  }



  @Named(TOTAL_TRANSITIONS)
  @Provides
  public Integer providesTotalTransitions(@ForArg(HBaseTaskOption.MAX_TIME_MILLIS)
                                          Optional<Long> maxTimeMillis,
                                          @ForArg(HBaseTaskOption.MAX_TRANSITIONS_PER_MINUTE)
                                          Optional<Integer> transitionsPerMinute) {
    return (int)(maxTimeMillis.get() * transitionsPerMinute.get() / TimeUnit.MINUTES.toMillis(1));
  }

}
