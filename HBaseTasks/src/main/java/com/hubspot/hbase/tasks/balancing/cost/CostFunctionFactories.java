package com.hubspot.hbase.tasks.balancing.cost;

import com.google.common.collect.ImmutableMap;

public class CostFunctionFactories {
  private CostFunctionFactories() {}

  public interface CostFunctionFactory {
    public CostFunction build(double weight, int totalTransitions);
  }

  private static final ImmutableMap<String, CostFunctionFactory> COST_FUNCTION_FACTORIES = ImmutableMap.<String, CostFunctionFactory>builder()
	  .put("DataSizeCost", new CostFunctionFactory() {
	    @Override
	    public CostFunction build(final double weight, final int totalTransitions) {
	      return new DataSizeCost(weight);
	    }
	  })
	  .put("LoadCost", new CostFunctionFactory() {
	    @Override
	    public CostFunction build(final double weight, final int totalTransitions) {
	      return new LoadCost(weight);
	    }
	  })
	  .put("LocalityCost", new CostFunctionFactory() {
	    @Override
	    public CostFunction build(final double weight, final int totalTransitions) {
	      return new LocalityCost(weight);
	    }
	  })
	  .put("RegionCountCost", new CostFunctionFactory() {
	    @Override
	    public CostFunction build(final double weight, final int totalTransitions) {
	      return new RegionCountCost(weight);
	    }
	  })
	  .put("TableDataBalanceCost", new CostFunctionFactory() {
	    @Override
	    public CostFunction build(final double weight, final int totalTransitions) {
	      return new TableDataBalanceCost(weight);
	    }
	  })
    .put("ProximateRegionKeyCost", new CostFunctionFactory() {
      @Override
      public CostFunction build(final double weight, final int totalTransitions) {
        return new ProximateRegionKeyCost(weight);
      }
    })
	  .put("TransitionCost", new CostFunctionFactory() {
      @Override
      public CostFunction build(final double weight, final int totalTransitions) {
        return new TransitionCost(weight, totalTransitions);
      }
    })
    .put("MigrationCost", new CostFunctionFactory() {
      @Override
      public CostFunction build(double weight, int totalTransitions) {
        return new MigrationCost(weight);
      }
    })
	  .build();

  public static CostFunctionFactory getCostFunctionFactory(final String name) {
    return COST_FUNCTION_FACTORIES.get(name);
  }
}
