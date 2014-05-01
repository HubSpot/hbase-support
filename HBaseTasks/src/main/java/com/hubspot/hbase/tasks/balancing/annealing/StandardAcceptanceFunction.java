package com.hubspot.hbase.tasks.balancing.annealing;

import java.util.Random;

public final class StandardAcceptanceFunction implements AcceptanceFunction {
  private final Random random = new Random();

  @Override
  public boolean acceptNewState(double originalScore, double newScore, double temperature) {
    if (newScore <= originalScore) {
      return true;
    }
    double probability = 1.0 / (1.0 + Math.exp((originalScore - newScore) / temperature));
    return random.nextDouble() < probability;
  }
}