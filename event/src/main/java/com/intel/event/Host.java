package com.intel.event;//////////////////////////////////////////////////////////////////////////////

import java.lang.management.ManagementFactory;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;

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
class Host {

    private static String machine;
    private static String workingDirectory;
    private static String processId;

    private Host() {}

    static String getMachineName() {
        if (machine == null)
            machine = lookupMachineName();
        return machine;
    }

    static String getWorkingDirectory() {
        if (workingDirectory == null)
            workingDirectory = lookupWorkingDirectory();
        return workingDirectory;
    }

    static String getProcessId() {
        if (processId == null)
            processId = lookupProcessId();
        return processId;
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
        String pid = name.substring(0, loc);
        return pid;
    }

}
