/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.webserver;

import com.microsoft.tang.annotations.DefaultImplementation;

/**
 * HttpServer interface
 */
@DefaultImplementation(HttpServerImpl.class)
public interface HttpServer {

  /**
   * start the server
   *
   * @throws Exception
   */
  public void start() throws Exception;

  /**
   * stop the server
   *
   * @throws Exception
   */
  public void stop() throws Exception;

  /**
   * get port number of the server
   *
   * @return
   */
  public int getPort();

  /**
   * Add a httpHandler to the server
   * @param httpHandler
   */
  public void addHttpHandler(HttpHandler httpHandler);
}
