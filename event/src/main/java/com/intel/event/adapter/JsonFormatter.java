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

package com.intel.event.adapter;
import com.intel.event.Event;
import com.intel.event.EventContext;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

/**
 * Formats events using JSON
 */
public class JsonFormatter {

    private static final DateFormat ISO_8601_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");

    static {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        ISO_8601_FORMAT.setTimeZone(tz);
    }

    @SuppressWarnings("unchecked")
    public static JSONObject toJson(EventContext ec) {
        JSONObject json = new JSONObject();
        json.put("name", ec.getName());
        json.put("data", ec.getData());
        return json;
    }

    @SuppressWarnings("unchecked")
    public static JSONObject toJson(Event e) {
        List<JSONObject> contexts = new ArrayList<>();
        EventContext current = e.getContext();
        while (current != null) {
            contexts.add(toJson(current));
            current = current.getParent();
        }
        JSONObject json = new JSONObject();
        json.put("id", e.getId());
        json.put("corId", e.getCorrelationId());
        json.put("severity", e.getSeverity().name());
        json.put("messageCode", e.getMessageCode());
        json.put("message", e.getMessage());
        json.put("machine", e.getMachine());
        json.put("user", e.getUser());
        json.put("threadId", e.getThreadId());
        json.put("threadName", e.getThreadName());
        json.put("date", ISO_8601_FORMAT.format(e.getDate()));
        json.put("substitutions", toJsonArray(e.getSubstitutions()));
        json.put("markers", toJsonArray(e.getMarkers()));
        json.put("errors", toJsonArray(e.getErrors()));
        json.put("contexts", contexts);
        json.put("data", e.getData());
        json.put("directory", e.getWorkingDirectory());
        json.put("process", e.getProcessId());
        return json;
    }

    private static JSONArray toJsonArray(Object[] array) {
        JSONArray jsonArray = new JSONArray();
        for (Object anArray : array) {
            if (anArray instanceof Throwable) {
                jsonArray.add(ExceptionUtils.getStackTrace((Throwable) anArray));

            }
            //noinspection unchecked
            jsonArray.add(String.valueOf(anArray));
        }
        return jsonArray;
    }

}
