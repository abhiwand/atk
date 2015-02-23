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

package com.intel.graphbuilder.driver.spark.rdd

import java.io.File

/**
 * Methods for validating the environment
 */
object EnvironmentValidator extends Serializable {

  //TODO: It seems this isn't needed anymore - delete and get rid of ispark-deps altogether?
  var skipEnvironmentValidation = true

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
          val parts = sparkClasspath.split("[: \n\r\t]+")
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
