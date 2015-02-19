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

package com.intel.intelanalytics.engine.spark.frame.plugins

import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.PythonRDDStorage
import com.intel.intelanalytics.domain.frame.CountWhereArgs
import scala.concurrent.ExecutionContext
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.LongValue

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Counts rows which meet criteria specified by a UDF predicate
 */
class CountWherePlugin extends SparkCommandPlugin[CountWhereArgs, LongValue] {

  override def name: String = "frame/count_where"

  /**
   * Return count of rows which meet criteria specified by a UDF predicate
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: CountWhereArgs)(implicit invocation: Invocation): LongValue = {
    val sourceFrame = engine.frames.expectFrame(arguments.frame)
    val pythonRDDStorage = new PythonRDDStorage(engine.frames)
    val pyRdd = pythonRDDStorage.createPythonRDD(sourceFrame.id, arguments.udf.function, sc)
    LongValue(pyRdd.map(s => JsonParser(new String(s)).convertTo[List[JsValue]]).flatMap(identity).count())
  }
}
