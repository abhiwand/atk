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

import java.util.Date;

/**
 * Encapsulates system data that were current at a particular moment in time
 */
class Instant {

    private final String threadName;
    private final String user;
    private final Date date;
    private final long threadId;

    Instant() {
        date = new Date();
        user = System.getProperty("user.name");
        threadId = Thread.currentThread().getId();
        threadName = Thread.currentThread().getName();
    }

    public String getThreadName() {
        return threadName;
    }

    public String getUser() {
        return user;
    }

    public Date getDate() {
        return date;
    }

    public long getThreadId() {
        return threadId;
    }
}

