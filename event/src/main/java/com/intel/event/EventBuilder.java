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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builder class for Events. Use this class to add additional information
 * to your events.
 */
public class EventBuilder {
    private final Severity severity;
    private final Enum message;
    private final String[] substitutions;
    private final EventContext context;
    private final Instant instant;
    private final List<String> markers = new ArrayList<>();
    private final Map<String,String> data = new HashMap<>();
    private final List<Throwable> errors = new ArrayList<>();

    /**
     * Constructor that takes all the data that are required in order
     * to construct a minimal Event.
     *
     * @param context the event context
     * @param severity the severity level
     * @param message the message key
     * @param substitutions the string substitutions for the message
     * @see Event
     * @throws IllegalArgumentException if severity or message are null
     */
    public EventBuilder(EventContext context,
                        Severity severity,
                        Enum message,
                        String... substitutions) {
        if (severity == null)
            throw new IllegalArgumentException("Severity cannot be null");
        if (message == null)
            throw new IllegalArgumentException("Message cannot be null");
        this.context = context;
        this.severity = severity;
        this.message = message;
        this.substitutions = substitutions;
        this.instant = new Instant();
    }

    /**
     * Add additional data to this event
     */
    public EventBuilder put(String key, String value) {
        data.put(key, value);
        return this;
    }

    /**
     * Add a marker (tag) to this event
     */
    public EventBuilder addMarker(String marker) {
        markers.add(marker);
        return this;
    }

    /**
     * Add a Throwable associated with this event
     */
    public EventBuilder addException(Throwable e) {
        errors.add(e);
        return this;
    }

    /**
     * Generate a read-only Event based on the parameters configured
     * by the builder.
     * @return the finished Event, which can then be passed to {@link EventLog#log}
     */
    public Event build() {
        String[] markers = this.markers.toArray(new String[this.markers.size()]);
        Throwable[] errors = this.errors.toArray(new Throwable[this.errors.size()]);
        return new Event(context, instant, new EventData(severity, errors, data, markers, message, substitutions));
    }
}
