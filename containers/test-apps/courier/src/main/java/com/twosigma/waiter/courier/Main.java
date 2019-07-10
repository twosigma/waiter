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

import java.util.logging.Logger;

public class Main {

    private final static Logger LOGGER = Logger.getLogger(Main.class.getName());

    // In order to run this, you need the alpn-boot-XXX.jar in the bootstrap classpath.
    public static void main(final String... args) throws Exception {
        final int port0 = args.length > 0 ? Integer.parseInt(args[0]) : 8080;
        final int port1 = args.length > 1 ? Integer.parseInt(args[1]) : 8081;

        LoggingConfig.initializeLogging();

        LOGGER.info("gRPC server configured to run on port " + port0);
        LOGGER.info("health-check (http) server configured to run on port " + port1);

        final GrpcServer grpcServer = new GrpcServer();
        grpcServer.start(port0);

        final HealthCheckServer healthCheckServer = new HealthCheckServer();
        healthCheckServer.start(port1);

        grpcServer.blockUntilShutdown();
    }
}
