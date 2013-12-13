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

import java.lang.management.ManagementFactory;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Information about the current host MACHINE
 */
class Host {

    private static String MACHINE;
    private static String WORKING_DIRECTORY;
    private static String PROCESS_ID;

    private Host() { }

    static String getMachineName() {
        if (MACHINE == null) {
            MACHINE = lookupMachineName();
        }
        return MACHINE;
    }

    static String getWorkingDirectory() {
        if (WORKING_DIRECTORY == null) {
            WORKING_DIRECTORY = lookupWorkingDirectory();
        }
        return WORKING_DIRECTORY;
    }

    static String getProcessId() {
        if (PROCESS_ID == null) {
            PROCESS_ID = lookupProcessId();
        }
        return PROCESS_ID;
    }

    private static String lookupMachineName() {
        String machineName;
        try {
            machineName = java.net.InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            machineName = "<UNKNOWN>";
        }
        return machineName;
    }

    private static String lookupWorkingDirectory() {
        Path currentRelativePath = Paths.get("");
        return currentRelativePath.toAbsolutePath().toString();
    }

    private static String lookupProcessId() {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        int loc = name.indexOf('@');
        return name.substring(0, loc);
    }

}
