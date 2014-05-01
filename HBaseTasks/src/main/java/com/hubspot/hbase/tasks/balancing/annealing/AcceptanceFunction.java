package com.hubspot.hbase.tasks.balancing.annealing;

public interface AcceptanceFunction {
  public boolean acceptNewState(double originalScore, double newScore, double temperature);
}