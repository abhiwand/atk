package com.intel.intelanalytics.engine.spark.util

import com.intel.event.EventLogging
import com.intel.intelanalytics.EventLoggingImplicits
import com.intel.intelanalytics.engine.plugin.Invocation

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
