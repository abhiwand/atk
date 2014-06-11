//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package com.intel.event;

import com.intel.event.adapter.ConsoleEventLog;

/**
 * EventLogger logs events to log files, message queues, or other destinations based on
 * configuration, in a manner similar to log4j, and in fact log4j is one possible destination
 * for EventLogger log messages.
 * <p/>
 * Unlike systems such as log4j, the EventLogger supports rich event data including nested
 * execution contexts, and delayed translation support, such that translation of log
 * entries can be done long after the message has been logged. This supports scenarios where
 * there are several languages in use among the people who wish to view the logs.
 *
 * @see EventContext
 * @see Event
 */
public class EventLogger {

    private static EventLog EVENT_LOG;

    private EventLogger() {
    }

    /**
     * Logs an event
     *
     * @param event the event to log
     */
    public static void log(Event event) {
        if (EVENT_LOG == null) {
            EVENT_LOG = new ConsoleEventLog();
        }
        EVENT_LOG.log(event);
    }

    /**
     * Logs an event at TRACE severity
     *
     * @param message       the message to log
     * @param substitutions string substitutions to replace placeholders in the message string
     */
    public static void trace(Enum message, String... substitutions) {
        log(EventContext.event(Severity.TRACE,
                message,
                substitutions).build());
    }

    /**
     * Logs an event at DEBUG severity
     *
     * @param message       the message to log
     * @param substitutions string substitutions to replace placeholders in the message string
     */
    public static void debug(Enum message, String... substitutions) {
        log(EventContext.event(Severity.DEBUG,
                message,
                substitutions).build());
    }

    /**
     * Logs an event at INFO severity
     *
     * @param message       the message to log
     * @param substitutions string substitutions to replace placeholders in the message string
     */
    public static void info(Enum message, String... substitutions) {
        log(EventContext.event(Severity.INFO,
                message,
                substitutions).build());
    }

    /**
     * Logs an event at WARN severity
     *
     * @param message       the message to log
     * @param substitutions string substitutions to replace placeholders in the message string
     */
    public static void warn(Enum message, String... substitutions) {
        log(EventContext.event(Severity.WARN,
                message,
                substitutions).build());
    }

    /**
     * Logs an event at ERROR severity
     *
     * @param message       the message to log
     * @param substitutions string substitutions to replace placeholders in the message string
     */
    public static void error(Enum message, String... substitutions) {
        log(EventContext.event(Severity.ERROR,
                message,
                substitutions).build());
    }

    /**
     * Logs an event at ERROR severity
     *
     * @param message       the message to log
     * @param error the error to log
     * @param substitutions string substitutions to replace placeholders in the message string
     */
    public static void error(Enum message, Throwable error, String... substitutions) {
        log(EventContext.event(Severity.ERROR,
                message,
                substitutions)
                .addException(error)
                .build());
    }

    /**
     * Logs an event at FATAL severity
     *
     * @param message       the message to log
     * @param substitutions string substitutions to replace placeholders in the message string
     */
    public static void fatal(Enum message, String... substitutions) {
        log(EventContext.event(Severity.FATAL,
                message,
                substitutions).build());
    }

    /**
     * Logs an event at FATAL severity
     *
     * @param message       the message to log
     * @param error the error to log
     * @param substitutions string substitutions to replace placeholders in the message string
     */
    public static void fatal(Enum message, Throwable error, String... substitutions) {
        log(EventContext.event(Severity.FATAL,
                message,
                substitutions)
                .addException(error)
                .build());
    }

    /**
     * Change the logging implementation. Generally this should be called once during application
     * startup.
     *
     * @param eventLog the logging implementation to use
     */
    public static void setImplementation(EventLog eventLog) {
        EventLogger.EVENT_LOG = eventLog;
    }
}
