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

package com.intel.intelanalytics.engine.spark.util

import java.text.NumberFormat
import java.util.Locale

import com.intel.event.{ EventContext, EventLogging }

/**
 * Make sure some settings get logged that are useful for debugging on clusters
 */
object EnvironmentLogger extends EventLogging {
  def log()(implicit eventContext: EventContext) = withContext("EnvironmentLogger") {
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
