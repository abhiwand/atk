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

package com.intel.intelanalytics.engine.spark.context

import com.intel.intelanalytics.shared.EventLogging
import scala.collection.mutable
import org.apache.spark.engine.{ ProgressPrinter, SparkProgressListener }

/**
 * This context management strategy creates a context per user if it doesn't exist, else returns the existing context
 * SparkContext is not a lightweight object, I had to increase max procs and max users limits in the OS to
 * create in the order of hundreds of SparkContetxs pre JVM
 */
object SparkContextPerUserStrategy extends SparkContextManagementStrategy with EventLogging {

  //TODO: take a look at spark.cleaner.ttl parameter, the doc says that this param is useful for long running contexts
  val contextMap = new mutable.HashMap[String, Context] with mutable.SynchronizedMap[String, Context] {}

  //TODO: how to run jobs as a particular user
  //TODO: Decide on spark context life cycle - should it be torn down after every operation,
  //or left open for some time, and reused if a request from the same user comes in?
  //Is there some way of sharing a context across two different Engine instances?

  override def getContext(user: String): Context = {
    contextMap.get(user) match {
      case Some(ctx) => ctx
      case None => {
        //we need to clean/update some properties to get rid of Spark's port binding problems
        //when creating multiple SparkContexts within the same JVM
        System.clearProperty("spark.driver.port") //need to clear this to get rid of port bind problems
        System.setProperty("spark.ui.port", String.valueOf(4041 + contextMap.size)) //need to uniquely set this to get rid of bind problems
        val context = sparkContextFactory.createSparkContext(configuration, "intel-analytics:" + user)
        val listener = new SparkProgressListener()
        val progressPrinter = new ProgressPrinter(listener)
        context.addSparkListener(listener)
        context.addSparkListener(progressPrinter)
        Context(context, listener)
        val ctx = Context(context, listener)
        contextMap += (user -> ctx)
        ctx
      }
    }
  }

  /**
   * stop all managed SparkContexts
   */
  override def cleanup(): Unit = {
    contextMap.keys.foreach { key =>
      contextMap(key).sparkContext.stop()
    }
  }

  /**
   * removes the SparkContext for the given user (key) if it exists
   */
  override def removeContext(user: String): Unit = {
    if (contextMap contains user) {
      contextMap(user).sparkContext.stop()
      contextMap -= user
    }
  }

  def getAllContexts(): List[Context] = {
    contextMap.values.toList
  }

}
