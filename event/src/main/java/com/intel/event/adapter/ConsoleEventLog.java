package com.intel.event.adapter;

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

import com.intel.event.Event;
import com.intel.event.EventLog;

/**
 * A simple event logger that logs events to System.out in JSON format
 *
 * This is the default logger used if no other is configured.
 */
public class ConsoleEventLog implements EventLog {

    @Override
    public void log(Event e) {
        System.out.println(e.toString());
    }
}