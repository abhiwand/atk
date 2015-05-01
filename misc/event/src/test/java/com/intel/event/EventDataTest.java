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

import org.junit.Test;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class EventDataTest {

    @Test(expected = IllegalArgumentException.class)
    public void EventData_requires_severity() {

        new EventData(null, null, null, null, 0, EventLoggerTest.Msg.SOMETHING_HAPPENED.toString());

    }

    @Test(expected = IllegalArgumentException.class)
    public void EventData_requires_message() {

        new EventData(Severity.INFO, null, null, null, 0, null, (String[])null);

    }

    public void EventData_defaults_empty_array_for_throwable() {

        assertThat(new EventData(Severity.INFO, null, null, null, 0, EventLoggerTest.Msg.SOMETHING_HAPPENED.toString())
                        .getErrors(),
                not(nullValue()));

    }

    public void EventData_defaults_empty_map_for_data() {

        assertThat(new EventData(Severity.INFO, null, null, null, 0, EventLoggerTest.Msg.SOMETHING_HAPPENED.toString())
                    .getData(),
                not(nullValue()));

    }

    public void EventData_defaults_empty_array_for_markers() {

        assertThat(new EventData(Severity.INFO, null, null, null, 0, EventLoggerTest.Msg.SOMETHING_HAPPENED.toString())
                        .getMarkers(),
                not(nullValue()));

    }


}
