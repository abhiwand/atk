package com.intel.intelanalytics.engine.spark.util

import java.text.NumberFormat
import java.util.Locale

import com.intel.event.EventLogging

/**
 * Make sure some settings get logged that are useful for debugging on clusters
 */
object EnvironmentLogger extends EventLogging {
  def log() = withContext("EnvironmentLogger") {
    withContext("environmentVariables") {
      System.getenv().keySet().toArray(Array[String]()).sorted.foreach(environmentVariable =>
        info(environmentVariable + "=" + System.getenv(environmentVariable))
      )
    }
    withContext("systemProperties") {
      System.getProperties.stringPropertyNames().toArray(Array[String]()).sorted.foreach(name => {
        info(name + "=" + System.getProperty(name))
      })
    }
    withContext("memory") {
      val formatter = NumberFormat.getInstance(Locale.US)
      info("free=" + formatter.format(Runtime.getRuntime.freeMemory()))
      info("total=" + formatter.format(Runtime.getRuntime.totalMemory()))
      info("max=" + formatter.format(Runtime.getRuntime.maxMemory()))
    }
  }
}
