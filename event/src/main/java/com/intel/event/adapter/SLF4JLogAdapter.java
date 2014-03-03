//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2013 Intel Corporation All Rights Reserved.
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

package com.intel.event.adapter;

import com.intel.event.Event;
import com.intel.event.EventLog;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

/**
 * EventLog implementation that sends events through SLF4J.
 */
public class SLF4JLogAdapter implements EventLog {

    @Override
    public void log(Event e) {
        Logger factory = LoggerFactory.getLogger(StringUtils.join(":", e.getContextNames()));
        MDC.setContextMap(e.getData());
        String[] markers = e.getMarkers();
        Marker marker = null;
        if (markers.length > 0) {
            marker = MarkerFactory.getDetachedMarker(markers[0]);
            for (int i = 1; i < markers.length; i++) {
                marker.add(MarkerFactory.getMarker(markers[i]));
            }
        }
        String logMessage = getLogMessage(e);
        Throwable[] errors = e.getErrors();
        Throwable firstError = errors.length > 0 ? errors[0] : null;
        switch (e.getSeverity()) {
        case INFO:
            factory.info(marker, logMessage, firstError);
            break;
        case WARN:
            factory.warn(marker, logMessage, firstError);
            break;
        case ERROR:
            factory.error(marker, logMessage, firstError);
            break;
        case DEBUG:
            factory.debug(marker, logMessage, firstError);
            break;
        case TRACE:
            factory.trace(marker, logMessage, firstError);
            break;
        case FATAL:
            factory.error(marker, logMessage, firstError);
            break;
        default:
            throw new RuntimeException("Unrecognized severity level: " + e.getSeverity().toString());
        }
    }

    private String getLogMessage(Event e) {
        StringBuilder builder = new StringBuilder("|");
        builder.append(e.getMessage());
        builder.append('|');
        builder.append(e.getMessageCode());
        builder.append("[");
        boolean first = true;
        for (String sub : e.getSubstitutions()) {
            if (!first) {
                builder.append(':');
            }
            builder.append(sub);
            first = false;
        }
        builder.append("]:[");
        first = true;
        for (String marker : e.getMarkers()) {
            if (!first) {
                builder.append(", ");
            }
            builder.append(marker);
            first = false;
        }
        builder.append(']');
        return builder.toString();
    }
}
