package com.intel.event;

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

import java.util.HashMap;
import java.util.Map;

/**
 * Encapsulates event-specific data
 */
class EventData {

    private final Severity severity;
    private final Throwable[] errors;
    private final Map<String, String> data;
    private final String[] markers;
    private final Enum message;
    private final String[] substitutions;

    /**
     * Parameters are stored for later use by Event, with the obvious name-based mapping to Event properties.
     *
     * @see Event
     */
    EventData(
        Severity severity,
        Throwable[] errors,
        Map<String,String> data,
        String[] markers,
        Enum message,
        String... substitutions) {
        if (severity == null)
            throw new IllegalArgumentException("Severity cannot be null");
        if (message == null)
            throw new IllegalArgumentException("Message cannot be null");
        this.severity = severity;
        this.errors = errors == null ? new Throwable[0]: errors;
        this.data = data == null ? new HashMap<String,String>() : data;
        this.markers = markers == null ? new String[0]: markers;
        this.message = message;
        this.substitutions = substitutions;
    }

    /**
     * Convenience constructor that creates an EventData with INFO severity.
     */
    public EventData(Enum message, String... substitutions) {
        this(Severity.INFO, null, null, null, message, substitutions);
    }

    public Severity getSeverity() {
        return severity;
    }

    public Map<String, String> getData() {
        return data;
    }

    public Enum getMessage() {
        return message;
    }

    public String[] getSubstitutions() {
        return substitutions;
    }

    public String[] getMarkers() {
        return markers;
    }

    public Throwable[] getErrors() {
        return errors;
    }
}
