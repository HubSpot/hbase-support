package com.hubspot.hbase.tasks.balancing.annealing.perturbers;

import com.google.inject.Inject;
import com.hubspot.hbase.tasks.balancing.annealing.AssignmentConfig;

import java.util.Random;

public class CombinedPerturber implements Perturber {
  private final RandomPerturber randomPerturber;
  private final SomewhatGreedyPerturber somewhatGreedyPerturber;
  private final Random random = new Random();

  @Inject
  public CombinedPerturber(final RandomPerturber randomPerturber, final SomewhatGreedyPerturber somewhatGreedyPerturber) {
    this.randomPerturber = randomPerturber;
    this.somewhatGreedyPerturber = somewhatGreedyPerturber;
  }

  @Override
  public AssignmentConfig perturb(final AssignmentConfig initial, final double temp) {
    if (random.nextDouble() < Math.pow(temp, .5)) {
      return randomPerturber.perturb(initial, temp);
    } else {
      return somewhatGreedyPerturber.perturb(initial, temp);
    }
  }
}
