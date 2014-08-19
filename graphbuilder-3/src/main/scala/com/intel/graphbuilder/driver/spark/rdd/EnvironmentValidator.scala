package com.intel.graphbuilder.driver.spark.rdd

import java.io.File

/**
 * Methods for validating the environment
 */
object EnvironmentValidator extends Serializable {

  var skipEnvironmentValidation = false

  /**
   * Validate that ispark-deps.jar is available to Spark.
   *
   * This jar is needed to connect to Titan from executors.
   */
  lazy val validateISparkDepsAvailable: Unit = {
    if (!skipEnvironmentValidation) {
      print("validating that ispark-deps.jar is available on the SPARK_CLASSPATH")
      val sparkClasspath = System.getenv("SPARK_CLASSPATH")
      if (sparkClasspath == null) {
        throw new RuntimeException("SPARK_CLASSPATH is NOT set.  Please set SPARK_CLASSPATH, include ispark-deps.jar in the classpath, and restart Spark")
      }
      else {
        if (!sparkClasspath.contains("ispark-deps.jar")) {
          throw new RuntimeException("SPARK_CLASSPATH does NOT contain ispark-deps.jar, please add, and restart Spark")
        }
        else {
          val parts = sparkClasspath.split("[: ]+")
          parts.foreach(part => {
            if (part.contains("ispark-deps.jar")) {
              val jar = new File(part)
              if (!jar.exists() || !jar.canRead) {
                throw new RuntimeException("ispark-deps.jar does NOT exist or permissions problem with: " + part + " please install jar or update SPARK_CLASSPATH and restart Spark")
              }
              print("ispark-deps.jar was found: " + part)
            }
          })
        }
      }
    }
  }
}
