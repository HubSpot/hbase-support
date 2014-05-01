package com.hubspot.hbase.tasks.balancing.annealing.perturbers;

import com.hubspot.hbase.tasks.balancing.annealing.AssignmentConfig;

public interface Perturber {
  public AssignmentConfig perturb(AssignmentConfig initial, double temp);
}
