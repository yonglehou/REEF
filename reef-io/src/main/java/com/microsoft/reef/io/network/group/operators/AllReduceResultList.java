package com.microsoft.reef.io.network.group.operators;

import java.util.LinkedList;
import java.util.List;

public class AllReduceResultList<T> {
  private LinkedList<T> valueList = null;

  public AllReduceResultList(List<T> vals) {
    if (vals != null) {
      valueList = new LinkedList<T>();
      for (int i = 0; i < vals.size(); i++) {
        valueList.add(vals.get(i));
      }
    }
  }

  public boolean isEmpty() {
    return valueList == null ? true : false;
  }

  public List<T> getValues() {
    return valueList;
  }
}
