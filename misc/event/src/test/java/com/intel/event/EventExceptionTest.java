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

public class EventExceptionTest {
    static enum Msg {
        SOMETHING
    }
    @Test(expected = EventException.class)
    public void EventException_with_event() throws EventException {
        throw new EventException(EventContext.event(Msg.SOMETHING).build());
    }

    @Test(expected = EventException.class)
    public void EventException_with_event_and_cause() throws EventException {
        throw new EventException(EventContext.event(Msg.SOMETHING).build(),
                new Exception());
    }
    @Test(expected = RuntimeEventException.class)
    public void RuntimeEventException_with_event() throws EventException {
        throw new RuntimeEventException(EventContext.event(Msg.SOMETHING).build());
    }

    @Test(expected = RuntimeEventException.class)
    public void RuntimeEventException_with_event_and_cause() throws EventException {
        throw new RuntimeEventException(EventContext.event(Msg.SOMETHING).build(),
                new Exception());
    }
}