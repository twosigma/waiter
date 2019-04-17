/*
 * Copyright (c) Two Sigma Open Source, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twosigma.waiter.courier;


import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.logging.Logger;

public class HealthCheckServer {

    private final static Logger LOGGER = Logger.getLogger(HealthCheckServer.class.getName());

    void start(final int port) throws IOException {
        LOGGER.info("starting health check server on port " + port);

        final InetSocketAddress address = new InetSocketAddress(port);
        final HttpServer server = HttpServer.create(address, 0);

        server.createContext("/status", new HealthCheckHandler());
        server.setExecutor(null);
        server.start();

        LOGGER.info("health check server started, listening on " + port);
    }

    private static class HealthCheckHandler implements HttpHandler {
        @Override
        public void handle(final HttpExchange httpExchange) throws IOException {

            final String correlationId = httpExchange.getRequestHeaders().getFirst("x-cid");
            LOGGER.info(correlationId + " received health check request");

            final Headers responseHeaders = httpExchange.getResponseHeaders();
            responseHeaders.add("content-type", "text/plain");
            responseHeaders.add("server", "courier-health-check/jdk" + System.getProperty("java.version"));
            if (correlationId != null) {
                responseHeaders.add("x-cid", correlationId);
            }

            final String response = "OK";
            httpExchange.sendResponseHeaders(200, response.length());

            final OutputStream os = httpExchange.getResponseBody();
            os.write(response.getBytes());
            os.close();

            LOGGER.info(correlationId + " responded with 200 OK");
        }
    }

}
