package com.twosigma.waiter.courier;

import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class LoggingConfig {

    protected static void initializeLogging() {
        try {
            // Programmatic configuration
            System.setProperty(
                "java.util.logging.SimpleFormatter.format",
                "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS.%1$tL %4$-7s (%2$s) %5$s %6$s%n");

            final ConsoleHandler consoleHandler = new ConsoleHandler();
            consoleHandler.setLevel(Level.FINEST);
            consoleHandler.setFormatter(new SimpleFormatter());

            final Logger rootLogger = Logger.getLogger("");
            rootLogger.setLevel(Level.FINEST);
            rootLogger.addHandler(consoleHandler);

            final Logger opencensusLogger = Logger.getLogger("io.opencensus");
            opencensusLogger.setLevel(Level.INFO);
            opencensusLogger.addHandler(consoleHandler);

            final Logger grpcContextLogger = Logger.getLogger("io.grpc.Context");
            grpcContextLogger.setLevel(Level.INFO);
            grpcContextLogger.addHandler(consoleHandler);
        } catch (Exception e) {
            // The runtime won't show stack traces if the exception is thrown
            e.printStackTrace();
        }
    }
}
