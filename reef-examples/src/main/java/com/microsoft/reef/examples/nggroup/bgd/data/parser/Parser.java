package com.microsoft.reef.examples.nggroup.bgd.data.parser;

import com.microsoft.reef.examples.nggroup.bgd.data.Example;

/**
 * Parses inputs into Examples.
 *
 * @param <T>
 */
public interface Parser<T> {

  public Example parse(final T input);

}
