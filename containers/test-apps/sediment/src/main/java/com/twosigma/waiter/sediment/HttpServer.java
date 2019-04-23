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
package com.twosigma.waiter.sediment;

import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class HttpServer {

    private final static Logger LOGGER = Logger.getLogger(HttpServer.class.getName());

    // In order to run this, you need the alpn-boot-XXX.jar in the bootstrap classpath.
    public static void main(String... args) throws Exception {
        final int port = args.length > 0 ? Integer.parseInt(args[0]) : 8080;
        LOGGER.info("server configured to run on port " + port);

        final Server server = new Server();

        final ServletContextHandler context =
            new ServletContextHandler(server, "/", ServletContextHandler.SESSIONS);
        context.addServlet(new ServletHolder(new Servlet()), "/");
        server.setHandler(context);

        final HttpConfiguration httpConfig = new HttpConfiguration();
        final List<ConnectionFactory> connectionFactories = new ArrayList<>();

        LOGGER.info("enabling http/1.1 server");
        connectionFactories.add(new HttpConnectionFactory(httpConfig));

        LOGGER.info("enabling h2c server");
        connectionFactories.add(new HTTP2CServerConnectionFactory(httpConfig));

        String defaultProtocol = System.getProperty("protocol");
        if (defaultProtocol == null) {
            defaultProtocol = "http/1.1";
        }
        LOGGER.info("default protocol is " + defaultProtocol);


        final ServerConnector serverConnector = new ServerConnector(server, connectionFactories.toArray(new ConnectionFactory[0]));
        serverConnector.setDefaultProtocol(defaultProtocol);
        serverConnector.setPort(port);
        server.addConnector(serverConnector);

        server.start();
        LOGGER.info("server started on port " + port);
        server.join();
    }
}