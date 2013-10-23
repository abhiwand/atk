package com.intel.event.adapter;

import com.intel.event.Event;
import com.intel.event.EventLog;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.*;

/* Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
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
            for(int i = 1; i < markers.length; i++) {
                marker.add(MarkerFactory.getMarker(markers[i]));
            }
        }
        String logMessage = getLogMessage(e);
        Throwable[] errors = e.getErrors();
        Throwable firstError = errors.length > 0 ? errors[0] : null;
        switch(e.getSeverity()) {
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
        String cls = e.getMessage().getClass().getName();
        StringBuilder builder = new StringBuilder(cls);
        builder.append('.');
        builder.append(e.getMessage().toString());
        for(String sub: e.getSubstitutions()) {
            builder.append(':');
            builder.append(sub);
        }
        builder.append(' ');
        builder.append('[');
        boolean first = true;
        for(String marker : e.getMarkers()) {
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
