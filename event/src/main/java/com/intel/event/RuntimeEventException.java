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

/**
 * An exception that packages an Event as its payload
 */
public class RuntimeEventException extends RuntimeException {

    private final Event event;

    /**
     * Constructor for an event
     */
    public RuntimeEventException(Event event) {
        this.event = event;
    }

    /**
     * Constructor for the case where the error was caused by an earlier
     * error.
     */
    public RuntimeEventException(Event event, Throwable cause) {
        super(cause);
        this.event = event;
    }

    /**
     * The event that caused this exception
     * @return the associated event
     */
    public Event getEvent() {
        return this.event;
    }
}
