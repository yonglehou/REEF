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

import java.util.ArrayList;
import java.util.List;

import com.microsoft.reef.io.network.nggroup.api.TaskNode;

/**
 * 
 */
public class TaskNodeImpl implements TaskNode {
  private final String taskId;
  private TaskNode parent;
  private final List<TaskNode> children = new ArrayList<>();
  
  private boolean running = false;

  public TaskNodeImpl(String taskId, TaskNode parent, boolean running) {
    super();
    this.taskId = taskId;
    this.parent = parent;
    this.running = running;
  }

  public TaskNodeImpl(String taskId, TaskNode parent) {
    super();
    this.taskId = taskId;
    this.parent = parent;
  }

  /**
   * @param taskId
   */
  public TaskNodeImpl(String taskId) {
    super();
    this.taskId = taskId;
  }

  @Override
  public void addChild(TaskNode child) {
    children.add(child);
  }

  @Override
  public void setParent(TaskNode parent) {
    this.parent = parent;
  }

  @Override
  public TaskNode getParent() {
    return parent;
  }

  @Override
  public String taskId() {
    return taskId;
  }

  @Override
  public void setRunning(boolean b) {
    running = b;
  }

  @Override
  public boolean isRunning() {
    return running;
  }

}
