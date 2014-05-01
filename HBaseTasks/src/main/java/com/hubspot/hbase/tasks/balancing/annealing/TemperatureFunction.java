package com.hubspot.hbase.tasks.balancing.annealing;

public interface TemperatureFunction {
  public double temperature(int currentStep, int nSteps);
}
