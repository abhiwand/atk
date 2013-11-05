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



import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Event encapsulates data related to something that happened at a particular point in time.
 */
public class Event {

    private static DateFormat iso8601Format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");

    static {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        iso8601Format.setTimeZone(tz);
    }

    private final String id = UUID.randomUUID().toString();
    private final EventData data;
    private final EventContext context;
    private final Instant instant;

    /**
     * The recommended way to create an Event is through EventContext.event or
     * EventLogger.(trace|debug|info|warn|error|fatal).
     *
     * @param context the event context for this event
     * @param instant system information at the time of the event
     * @param data additional data specific to the event
     */
    public Event(EventContext context,
                 Instant instant,
                 EventData data) {

        if (instant == null)
            throw new IllegalArgumentException("Instant cannot be null");
        if (data == null)
            throw new IllegalArgumentException("Data cannot be null");

        this.context = context;
        this.instant = instant;
        this.data = data;
    }

    /**
     * Identifier for this event. Event IDs are universally unique.
     */
    public String getId() {
        return id;
    }

    /**
     * Contextual data associated with the event. These data are user-defined, and specific
     * to the needs of the code that created the event.
     *
     * @return a map of useful contextual information at the time the event was created
     */
    public Map<String,String> getData() {
        HashMap<String,String> map = new HashMap<>();
        if (context != null)
            map.putAll(context.getData());
        if (data.getData() != null)
            map.putAll(data.getData());
        return map;
    }

    /**
     * The names of all the contexts in effect on the thread when this event was created.
     */
    public String[] getContextNames() {
        ArrayList<String> names = new ArrayList<>();
        EventContext current = context;
        while(current != null) {
            names.add(current.getName());
            current = current.getParent();
        }
        Collections.reverse(names);
        return names.toArray(new String[names.size()]);
    }

    /**
     * Returns the details of the event and contexts in JSON format
     */
    @Override
    public String toString() {
        JSONObject json = toJson();
        return json.toJSONString();
    }

    JSONObject toJson() {
        List<JSONObject> contexts = new ArrayList<>();
        EventContext current = context;
        while (current != null) {
            contexts.add(current.toJson());
            current = current.getParent();
        }
        JSONObject json = new JSONObject();
        json.put("id", getId());
        json.put("corId", getCorrelationId());
        json.put("severity", getSeverity().name());
        json.put("message", getMessage().name());
        json.put("machine", getMachine());
        json.put("user", getUser());
        json.put("threadId", getThreadId());
        json.put("threadName", getThreadName());
        json.put("date", iso8601Format.format(getDate()));
        json.put("substitutions", toJsonArray(getSubstitutions()));
        json.put("markers", toJsonArray(getMarkers()));
        json.put("errors", toJsonArray(getErrors()));
        json.put("contexts", contexts);
        json.put("data", getData());
        json.put("directory", getWorkingDirectory());
        json.put("process", getProcessId());
        return json;
    }

    private JSONArray toJsonArray(Object[] array) {
        JSONArray jsonArray = new JSONArray();
        for (int i = 0; i < array.length; i++) {
            jsonArray.add(String.valueOf(array[i]));
        }
        return jsonArray;
    }

    /**
     * Returns the instant at which the event occurred.
     */
    public Date getDate() {
        return instant.getDate();
    }

    /**
     * Returns the name of the thread on which the event occurred.
     */
    private String getThreadName() {
        return instant.getThreadName();
    }

    /**
     * The numeric ID of the thread on which the event occurred.
     */
    private long getThreadId() {
        return instant.getThreadId();
    }

    /**
     * The logged in user for the application in which the event occurred
     */
    private String getUser() {
        return instant.getUser();
    }

    /**
     * The hostname of the machine on which the event occurred
     */
    private String getMachine() {
        return Host.getMachineName();
    }

    /**
     * The correlation ID the event is associated with.
     * @return the correlation ID
     * @see com.intel.event.EventContext#getCorrelationId()
     */
    public String getCorrelationId() {
        return context == null ? getId() : context.getCorrelationId();
    }

    /**
     * Returns the severity level associated with the event
     */
    public Severity getSeverity() {
        return data.getSeverity();
    }


    /**
     * Returns the message constant associated with the event
     */
    public Enum getMessage() {
        return data.getMessage();
    }

    /**
     * Returns the string substitutions that are associated with the message
     */
    public String[] getSubstitutions() {
        return data.getSubstitutions();
    }

    /**
     * Returns any markers that are associated with the event. Markers are simple
     * tags that can be used to categorize the event for later analysis or
     * special treatment by log handlers.
     */
    public String[] getMarkers() {
        return data.getMarkers();
    }

    /**
     * Returns all the errors associated with the event.
     */
    public Throwable[] getErrors() {
        return data.getErrors();
    }

    public String getWorkingDirectory() {
        return Host.getWorkingDirectory();
    }

    public String getProcessId() {
        return Host.getProcessId();
    }
}

