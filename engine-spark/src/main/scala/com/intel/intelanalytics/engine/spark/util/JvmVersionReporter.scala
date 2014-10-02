package com.intel.intelanalytics.engine.spark.util

import com.intel.event.EventLogging

/**
 * Logs info about the JVM, giving a warning if it sees OpenJDK
 */
object JvmVersionReporter extends EventLogging {

  def check(): Unit = withContext("JvmVersionReporter") {
    info(System.getProperty("java.runtime.name") + " -- " + System.getProperty("java.vm.name") + " -- " + System.getProperty("java.version"))
    if (System.getProperty("java.vm.name").contains("OpenJDK")) {
      warn("Did NOT expect OpenJDK!")
    }
  }
}
