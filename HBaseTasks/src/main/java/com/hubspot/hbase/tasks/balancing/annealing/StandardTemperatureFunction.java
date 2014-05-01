package com.hubspot.hbase.tasks.balancing.annealing;

public enum StandardTemperatureFunction implements TemperatureFunction {
  LINEAR_FUNCTION {
    @Override
    public double temperature(int currentStep, int nSteps) {
      return ((double) (nSteps - 1 - currentStep)) / nSteps;
    }
  },
  QUADRATIC_FUNCTION {
    @Override
    public double temperature(int currentStep, int nSteps) {
      int numerator = nSteps - 1 - currentStep;
      numerator *= numerator;
      return ((double) numerator) / (nSteps * nSteps);
    }
  },
  FLAT {
    @Override
    public double temperature(int currentStep, int nSteps) {
      return 0.5;
    }
  }

}
