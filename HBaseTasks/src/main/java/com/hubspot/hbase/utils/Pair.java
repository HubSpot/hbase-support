package com.hubspot.hbase.utils;

import com.google.common.base.Function;
import com.google.common.base.Objects;

import javax.annotation.Nullable;


/**
 * An immutable 2-tuple with value-equals semantics.
 *
 * @param <A> The type of the 1st item in the pair.
 * @param <B> The type of the 2nd item in the pair.
 * @author William Farner
 */
public class Pair<A, B> {

  @Nullable
  private final A first;
  @Nullable
  private final B second;

  /**
   * Creates a new pair.
   *
   * @param first  The first value.
   * @param second The second value.
   */
  public Pair(@Nullable A first, @Nullable B second) {
    this.first = first;
    this.second = second;
  }

  @Nullable
  public A getFirst() {
    return first;
  }

  @Nullable
  public B getSecond() {
    return second;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof Pair)) {
      return false;
    }

    Pair that = (Pair) o;
    return Objects.equal(this.first, that.first) && Objects.equal(this.second, that.second);
  }

  @Override
  public String toString() {
    return String.format("(%s, %s)", getFirst(), getSecond());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(first, second);
  }

  /**
   * Creates a function that can extract the first item of pairs of the given type parametrization.
   *
   * @param <S> The type of the 1st item in the pair.
   * @return A function that will extract the 1st item in a pair.
   */
  public static <S> Function<Pair<? extends S, ?>, S> first() {
    return new Function<Pair<? extends S, ?>, S>() {
      @Override
      public S apply(Pair<? extends S, ?> pair) {
        return pair.first;
      }
    };
  }

  /**
   * Creates a function that can extract the second item of pairs of the given type parametrization.
   *
   * @param <T> The type of the 2nd item in the pair.
   * @return A function that will extract the 2nd item in a pair.
   */
  public static <T> Function<Pair<?, ? extends T>, T> second() {
    return new Function<Pair<?, ? extends T>, T>() {
      @Override
      public T apply(Pair<?, ? extends T> pair) {
        return pair.second;
      }
    };
  }

  /**
   * Convenience method to create a pair.
   *
   * @param a   The first value.
   * @param b   The second value.
   * @param <A> The type of the 1st item in the pair.
   * @param <B> The type of the 2nd item in the pair.
   * @return A new pair of [a, b].
   */
  public static <A, B> Pair<A, B> of(@Nullable A a, @Nullable B b) {
    return new Pair<A, B>(a, b);
  }
}