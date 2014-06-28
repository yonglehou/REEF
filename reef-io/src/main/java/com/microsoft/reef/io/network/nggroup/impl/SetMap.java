/*
 * Copyright 2013 Microsoft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.io.network.nggroup.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class SetMap<K, V> {
  private final Map<K, Set<V>> map = new HashMap<>();

  public boolean containsKey(final K key) {
    return map.containsKey(key);
  }

  public boolean contains(final K key, final V value) {
    if(!containsKey(key)) {
      return false;
    }
    return map.get(key).contains(value);
  }

  public Set<V> get(final K key){
    if(map.containsKey(key)) {
      return map.get(key);
    } else {
      return Collections.emptySet();
    }
  }

  public void add(final K key, final V value) {
    final Set<V> values;
    if(!map.containsKey(key)) {
      values = new HashSet<>();
      map.put(key, values);
    }
    else {
      values = map.get(key);
    }
    values.add(value);
  }

  public boolean remove(final K key, final V value) {
    if(!map.containsKey(key)) {
      return false;
    }
    final Set<V> set = map.get(key);
    final boolean retVal = set.remove(value);
    if(set.isEmpty()) {
      map.remove(key);
    }
    return retVal;
  }

  /**
   * @param key
   * @return
   */
  public int count(final K key) {
    if(!containsKey(key)) {
      return 0;
    } else {
      return map.get(key).size();
    }
  }

  /**
   * @param key
   */
  public Set<V> remove(final MsgKey key) {
    return map.remove(key);
  }

  public Set<K> keySet(){
    return map.keySet();
  }
}
