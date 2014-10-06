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

package com.microsoft.reef.examples.hellohttp;

import com.microsoft.reef.util.CommandUtils;
import com.microsoft.reef.webserver.HttpHandler;
import com.microsoft.reef.webserver.ParsedHttpRequest;
import com.microsoft.tang.InjectionFuture;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.wake.EventHandler;
import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Http Event handler for Shell Command
 */
@Unit
class HttpServerShellCmdtHandler implements HttpHandler {
    /**
     * Standard Java logger.
     */
    private static final Logger LOG = Logger.getLogger(HttpServerShellCmdtHandler.class.getName());

    private static final int WAIT_TIMEOUT = 10 * 1000;

    private static final int WAIT_TIME = 50;

    /**
     *  ClientMessageHandler
     */
    private final InjectionFuture<HttpShellJobDriver.ClientMessageHandler> messageHandler;

    /**
     * uri specification
     */
    private String uriSpecification = "Command";

    /**
     * output for command
     */
    private String cmdOutput = null;

    /**
     * HttpServerDistributedShellEventHandler constructor.
     */
    @Inject
    public HttpServerShellCmdtHandler(final InjectionFuture<HttpShellJobDriver.ClientMessageHandler> messageHandler) {
        this.messageHandler = messageHandler;
    }

    /**
     * returns URI specification for the handler
     *
     * @return
     */
    @Override
    public String getUriSpecification() {
        return uriSpecification;
    }

    /**
     * set URI specification
     * @param s
     */
    public void setUriSpecification(final String s) {
        uriSpecification = s;
    }

    /**
     * it is called when receiving a http request
     *
     * @param parsedHttpRequest
     * @param response
     */
    @Override
    public final synchronized void onHttpRequest(final ParsedHttpRequest parsedHttpRequest, final HttpServletResponse response) throws IOException, ServletException {
        LOG.log(Level.INFO, "HttpServeShellCmdtHandler in webserver onHttpRequest is called: {0}", parsedHttpRequest.getRequestUri());
        final Map<String, List<String>> queries = parsedHttpRequest.getQueryMap();
        final String queryStr = parsedHttpRequest.getQueryString();

        if (parsedHttpRequest.getTargetEntity().equalsIgnoreCase("Evaluators")) {
            final byte[] b = HttpShellJobDriver.CODEC.encode(queryStr);
            LOG.log(Level.INFO, "HttpServeShellCmdtHandler call HelloDriver onCommand(): {0}", queryStr);
            messageHandler.get().onNext(b);

            notify();

            final long endTime = System.currentTimeMillis() + WAIT_TIMEOUT;
            while (cmdOutput == null) {
                final long waitTime = endTime - System.currentTimeMillis();
                if (waitTime <= 0) {
                    break;
                }

                try {
                    wait(WAIT_TIME);
                } catch (final InterruptedException e) {
                    LOG.log(Level.WARNING, "HttpServeShellCmdtHandler onHttpRequest InterruptedException: {0}", e);
                }
            }
            response.getOutputStream().write(cmdOutput.getBytes(Charset.forName("UTF-8")));
            cmdOutput = null;
        } else if (parsedHttpRequest.getTargetEntity().equalsIgnoreCase("Driver")) {
            final String cmdOutput = CommandUtils.runCommand(queryStr);
            response.getOutputStream().write(cmdOutput.getBytes(Charset.forName("UTF-8")));
        }
    }

    /**
     * called after shell command is completed
     * @param message
     */
    public final synchronized void onHttpCallback(byte[] message) {
        final long endTime = System.currentTimeMillis() + WAIT_TIMEOUT;
        while (cmdOutput != null) {
            final long waitTime = endTime - System.currentTimeMillis();
            if (waitTime <= 0) {
                break;
            }

            try {
                wait(WAIT_TIME);
            } catch(final InterruptedException e) {
                LOG.log(Level.WARNING, "HttpServeShellCmdtHandler onHttpCallback InterruptedException: {0}", e);
            }
        }
        LOG.log(Level.INFO, "HttpServeShellCmdtHandler OnCallback: {0}", HttpShellJobDriver.CODEC.decode(message));
        cmdOutput = HttpShellJobDriver.CODEC.decode(message);

        notify();
    }

    /**
     * Handler for client to call back
     */
    final class ClientCallBackHandler implements EventHandler<byte[]> {
        @Override
        public void onNext(final byte[] message) {
            HttpServerShellCmdtHandler.this.onHttpCallback(message);
        }
    }
}
