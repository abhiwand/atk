/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.taproot.analytics.engine.util

import com.intel.taproot.event.EventLogging
import com.intel.taproot.analytics.EventLoggingImplicits
import com.intel.taproot.analytics.engine.plugin.Invocation

/**
 * Logs info about the JVM, giving a warning if it sees OpenJDK
 */
object JvmVersionReporter extends EventLogging with EventLoggingImplicits {

  def check()(implicit invocation: Invocation): Unit = withContext("JvmVersionReporter") {
    info(System.getProperty("java.runtime.name") + " -- " + System.getProperty("java.vm.name") + " -- " + System.getProperty("java.version"))
    if (System.getProperty("java.vm.name").contains("OpenJDK")) {
      warn("Did NOT expect OpenJDK!")
    }
  }
}
