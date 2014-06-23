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

package com.intel.intelanalytics.engine.spark.plugin

import com.intel.intelanalytics.engine.Engine
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.SparkContext
import spray.json.JsObject

import scala.concurrent.ExecutionContext

/**
 * Captures details of a particular invocation of a command. This instance is passed to the
 * command's execute method along with the (converted) arguments supplied by the caller.
 *
 * @param engine an instance of the Engine for use by the command
 * @param user the calling user
 * @param commandId the ID assigned to this command execution
 * @param executionContext the Scala execution context in use
 * @param arguments the original JSON arguments, unconverted
 * @param sparkContext a method that can be called to get direct access to a SparkContext
 */
case class SparkInvocation(engine: Engine,
                           user: UserPrincipal,
                           commandId: Long,
                           executionContext: ExecutionContext,
                           arguments: Option[JsObject],
                           sparkContext: SparkContext
                           ) extends Invocation
