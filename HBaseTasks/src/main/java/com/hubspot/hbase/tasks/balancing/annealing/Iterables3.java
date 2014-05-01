package com.hubspot.hbase.tasks.balancing.annealing;

import java.util.Iterator;
import java.util.Random;

public final class Iterables3 {
  private static final Random random = new Random();

  private Iterables3() {}

  public static <T> T pickRandom(Iterable<T> iterable) {
    final Iterator<T> iterator = iterable.iterator();
    if (!iterator.hasNext()) return null;
    T result = iterator.next();
    for (int i = 0; iterator.hasNext(); ++i) {
      final T current = iterator.next();
      if (random.nextDouble() < (1.0d / (i + 2.0))) {
        result = current;
      }
    }
    return result;
  }
}
