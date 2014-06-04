//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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

package com.intel.intelanalytics.engine

import java.util.Date
import org.apache.spark.SparkContext
import scala.concurrent.Lock
import org.scalatest.{ FlatSpec, BeforeAndAfter }
import org.apache.log4j.{ Logger, Level }
import org.scalacheck.Prop.{ False, True }

trait TestingSparkContext extends FlatSpec with BeforeAndAfter {

  val useGlobalSparkContext: Boolean = System.getProperty("useGlobalSparkContext", "false").toBoolean

  var sc: SparkContext = null

  before {
    if (useGlobalSparkContext) {
      sc = TestingSparkContext.sc
    }
    else {
      TestingSparkContext.lock.acquire()
      sc = TestingSparkContext.createSparkContext
    }
  }

  /**
   * Clean up after the test is done
   */
  after {
    if (!useGlobalSparkContext) cleanupSpark()
  }

  /**
   * Shutdown spark and release the lock
   */
  private def cleanupSpark(): Unit = {
    try {
      if (sc != null) {
        sc.stop()
      }
    }
    finally {
      // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
      System.clearProperty("spark.driver.port")

      TestingSparkContext.lock.release()
    }
  }
}

object TestingSparkContext {
  private val lock = new Lock()
  private lazy val sc: SparkContext = createSparkContext

  def createSparkContext: SparkContext = {
    setLogLevels(Level.WARN, Seq("spark", "org.eclipse.jetty", "akka"))
    new SparkContext("local", "test " + new Date())
  }

  private def setLogLevels(level: org.apache.log4j.Level, loggers: TraversableOnce[String]): Unit = {
    loggers.foreach(loggerName => Logger.getLogger(loggerName).setLevel(level))
  }
}