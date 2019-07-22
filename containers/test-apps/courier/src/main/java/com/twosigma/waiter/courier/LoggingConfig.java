package com.twosigma.waiter.courier;

import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class LoggingConfig {

    public static void initializeLogging() {
        try {
            // Programmatic configuration
            System.setProperty(
                "java.util.logging.SimpleFormatter.format",
                "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS.%1$tL %4$-7s (%2$s) %5$s %6$s%n");

            final ConsoleHandler consoleHandler = new ConsoleHandler();
            consoleHandler.setLevel(Level.FINEST);
            consoleHandler.setFormatter(new SimpleFormatter());

            initializeLogging(consoleHandler);
        } catch (final Exception ex) {
            // The runtime won't show stack traces if the exception is thrown
            ex.printStackTrace();
        }
    }

    public static void initializeLogging(final Handler handler) {
        try {
            final Logger rootLogger = Logger.getLogger("");
            rootLogger.setLevel(Level.FINEST);
            rootLogger.addHandler(handler);

            final Logger opencensusLogger = Logger.getLogger("io.opencensus");
            opencensusLogger.setLevel(Level.INFO);
            opencensusLogger.addHandler(handler);

            final Logger grpcContextLogger = Logger.getLogger("io.grpc.Context");
            grpcContextLogger.setLevel(Level.INFO);
            grpcContextLogger.addHandler(handler);
        } catch (final Exception ex) {
            // The runtime won't show stack traces if the exception is thrown
            ex.printStackTrace();
        }
    }
}
