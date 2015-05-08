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

package com.intel.intelanalytics.engine.spark.gc

import java.util.concurrent.TimeUnit

import com.intel.intelanalytics.UnitReturn
import com.intel.intelanalytics.engine.plugin.{ Invocation, CommandPlugin }
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.typesafe.config.ConfigFactory

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Plugin that executes a single instance of garbage collection with user timespans specified at runtime
 */
class GarbageCollectionPlugin extends CommandPlugin[GarbageCollectionArgs, UnitReturn] {
  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   *  This method does not execute against a specific object but is instead a static method
   *  Will be called against an explicitly written method
   *
   */
  override def name: String = "_admin:/_explicit_garbage_collection"

  /**
   * Execute a single instance of garbage collection
   */
  override def execute(arguments: GarbageCollectionArgs)(implicit context: Invocation): UnitReturn = {
    val dataDeleteAge = arguments.ageToDeleteData match {
      case Some(age) => stringToMilliseconds(age)
      case None => SparkEngineConfig.gcAgeToDeleteData
    }
    GarbageCollector.singleTimeExecution(dataDeleteAge)
    new UnitReturn
  }

  /**
   * Utilize the typesafe SimpleConfig.parseDuration method to allow this plugin to use the typesafe duration format
   * just like the config
   * @param str string value to convert into milliseconds
   * @return milliseconds age range from the string object
   */
  def stringToMilliseconds(str: String): Long = {
    val config = ConfigFactory.parseString(s"""string_value = "$str" """)
    config.getDuration("string_value", TimeUnit.MILLISECONDS)
  }
}
