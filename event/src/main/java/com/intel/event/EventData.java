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

package com.intel.event;

import java.text.MessageFormat;
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
    private final int messageCode;
    private final String message;
    private final String[] substitutions;

    /**
     * Parameters are stored for later use by Event, with the obvious name-based mapping to Event properties.
     *
     * @see Event
     */
    EventData(
            Severity severity,
            Throwable[] errors,
            Map<String, String> data,
            String[] markers,
            int messageCode,
            String message,
            String... substitutions) {
        if (severity == null) {
            throw new IllegalArgumentException("Severity cannot be null");
        }
        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }
        this.severity = severity;
        this.errors = errors == null ? new Throwable[0] : errors;
        this.data = data == null ? new HashMap<String, String>() : data;
        this.markers = markers == null ? new String[0] : markers;
        this.messageCode = messageCode;
        this.message = MessageFormat.format(message, substitutions);
        this.substitutions = substitutions;
    }

    public Severity getSeverity() {
        return severity;
    }

    public Map<String, String> getData() {
        return data;
    }

    public String getMessage() {
        return message;
    }

    public int getMessageCode() { return messageCode; }

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
