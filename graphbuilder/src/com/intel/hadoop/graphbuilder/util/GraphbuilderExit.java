package com.intel.hadoop.graphbuilder.util;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;

public class GraphbuilderExit {

    public static void graphbuilderFatalExitException(StatusCode statusCode, String message, Logger log, Exception e) {
        log.fatal(message);
        System.err.println(message);  // two places? hey why not, make this stuff easy to find
        System.err.println(statusCode.getMessage());
        System.err.println(e.getMessage());
        e.printStackTrace(System.err);
        System.exit(statusCode.getStatus());
    }

    public static void graphbuilderFatalExitNoException(StatusCode statusCode, String message, Logger log) {
        log.fatal(message);
        System.err.println(message);  // two places? hey why not, make this stuff easy to find
        System.err.println(statusCode.getMessage());
        System.exit(statusCode.getStatus());
    }
}