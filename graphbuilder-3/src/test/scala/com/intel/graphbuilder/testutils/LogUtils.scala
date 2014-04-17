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

package com.intel.graphbuilder.testutils

import org.apache.log4j.{Logger, Level}

/**
 * Utility methods related to logging in Unit testing.
 * <p>
 * Logging of underlying libraries can get annoying in unit
 * tests so it is nice to be able to change easily.
 * </p>
 */
object LogUtils {

  /**
   * Turn down logging since Spark gives so much output otherwise.
   */
  def silenceSpark() {
    setLogLevels(Level.WARN, Seq("spark", "org.eclipse.jetty", "akka"))
  }

  /**
   * Turn down logging for Titan
   */
  def silenceTitan() {
    setLogLevels(Level.WARN, Seq("com.thinkaurelius"))
    setLogLevels(Level.ERROR, Seq("com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx"))
  }

  private def setLogLevels(level: org.apache.log4j.Level, loggers: TraversableOnce[String]): Unit = {
    loggers.foreach(loggerName => Logger.getLogger(loggerName).setLevel(level))
  }

}
