//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

/**
 * Event encapsulates data related to something that happened at a particular point in time.
 */
public class Event {


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

        if (instant == null) {
            throw new IllegalArgumentException("Instant cannot be null");
        }
        if (data == null) {
            throw new IllegalArgumentException("Data cannot be null");
        }
        this.context = context;
        this.instant = instant;
        this.data = data;
    }

    /**
     * Identifier for this event. Event IDs are universally unique.
     *
     * @return the id
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
    public Map<String, String> getData() {
        HashMap<String, String> map = new HashMap<>();
        if (context != null) {
            map.putAll(context.getData());
        }
        if (data.getData() != null) {
            map.putAll(data.getData());
        }
        map.put("id", id);
        if (context != null) {
            map.put("corId", context.getCorrelationId());
        }
        return map;
    }

    /**
     * The names of all the contexts in effect on the thread when this event was created.
     *
     * @return the context names
     */
    public String[] getContextNames() {
        ArrayList<String> names = new ArrayList<>();
        EventContext current = context;
        while (current != null) {
            names.add(current.getName());
            current = current.getParent();
        }
        Collections.reverse(names);
        return names.toArray(new String[names.size()]);
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
    public String getThreadName() {
        return instant.getThreadName();
    }

    /**
     * The numeric ID of the thread on which the event occurred.
     */
    public long getThreadId() {
        return instant.getThreadId();
    }

    /**
     * The logged in user for the application in which the event occurred
     */
    public String getUser() {
        return instant.getUser();
    }

    /**
     * The hostname of the machine on which the event occurred
     */
    public String getMachine() {
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
    public int getMessageCode() {
        return data.getMessageCode();
    }

    /**
     * Returns the message associated with the event
     */
    public String getMessage() {
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

    public EventContext getContext() { return context; }
}
