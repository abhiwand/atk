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

/* Copyright (C) 2014 Intel Corporation.
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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;

@RunWith(JUnit4.class)
public class EventLoggerTest {

    FakeLogger logger;

    @Before
    public void before() {
        logger = new FakeLogger();
        EventLogger.setImplementation(logger);
    }

    @After
    public void after() {
        EventLogger.setImplementation(null);
    }

    static enum Msg {
        SOMETHING_HAPPENED
    }

    static class FakeLogger implements EventLog {

        List<Event> events = new ArrayList<>();

        @Override
        public void log(Event e) {
            events.add(e);
        }
    }


    @Test
    public void EventLogger_throws_if_no_implementation_set() throws UnsupportedEncodingException {
        EventLogger.setImplementation(null);
        PrintStream out = System.out;
        try {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            System.setErr(new PrintStream(stream));
            EventLogger.log(
                    EventContext.event(Msg.SOMETHING_HAPPENED).build()
            );
            String result = stream.toString("UTF-8");
            assertThat(result, containsString("not configured"));
        } finally {
            System.setOut(out);
        }
    }

    @Test
    public void info_logs_info() {
        EventLogger.info(Msg.SOMETHING_HAPPENED);

        assertThat(logger.events.size(), is(equalTo(1)));
        Event e = logger.events.get(0);

        assertThat(e.getSeverity(), is(equalTo(Severity.INFO)));

    }

    @Test
    public void trace_logs_trace() {
        EventLogger.trace(Msg.SOMETHING_HAPPENED);

        assertThat(logger.events.size(), is(equalTo(1)));
        Event e = logger.events.get(0);

        assertThat(e.getSeverity(), is(equalTo(Severity.TRACE)));

    }

    @Test
    public void debug_logs_debug() {
        EventLogger.debug(Msg.SOMETHING_HAPPENED);

        assertThat(logger.events.size(), is(equalTo(1)));
        Event e = logger.events.get(0);

        assertThat(e.getSeverity(), is(equalTo(Severity.DEBUG)));

    }

    @Test
    public void warn_logs_warn() {
        EventLogger.warn(Msg.SOMETHING_HAPPENED);

        assertThat(logger.events.size(), is(equalTo(1)));
        Event e = logger.events.get(0);

        assertThat(e.getSeverity(), is(equalTo(Severity.WARN)));

    }

    @Test
    public void error_logs_error() {
        EventLogger.error(Msg.SOMETHING_HAPPENED);

        assertThat(logger.events.size(), is(equalTo(1)));
        Event e = logger.events.get(0);

        assertThat(e.getSeverity(), is(equalTo(Severity.ERROR)));

    }

    @Test
    public void fatal_logs_fatal() {
        EventLogger.fatal(Msg.SOMETHING_HAPPENED);

        assertThat(logger.events.size(), is(equalTo(1)));
        Event e = logger.events.get(0);

        assertThat(e.getSeverity(), is(equalTo(Severity.FATAL)));

    }

    @Test
    public void EventLogger_constructor_is_private() throws IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException {
        Constructor constructor = EventLogger.class.getDeclaredConstructor();
        assertThat(Modifier.isPrivate(constructor.getModifiers()), is(true));
        constructor.setAccessible(true);
        constructor.newInstance();
    }

    @Test
    public void error_logs_error_with_exception() {
        EventLogger.error(Msg.SOMETHING_HAPPENED, new Exception("oops"));

        assertThat(logger.events.size(), is(equalTo(1)));
        Event e = logger.events.get(0);

        assertThat(e.getSeverity(), is(equalTo(Severity.ERROR)));
        assertThat(e.getErrors()[0].getMessage(), is(equalTo("oops")));
    }

    @Test
    public void fatal_logs_fatal_with_exception() {
        EventLogger.fatal(Msg.SOMETHING_HAPPENED, new Exception("oops"));

        assertThat(logger.events.size(), is(equalTo(1)));
        Event e = logger.events.get(0);

        assertThat(e.getSeverity(), is(equalTo(Severity.FATAL)));
        assertThat(e.getErrors()[0].getMessage(), is(equalTo("oops")));

    }
}
