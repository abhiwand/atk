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

package com.intel.intelanalytics.engine.plugin

import com.intel.intelanalytics.engine.{ CommandStorage, Engine }
import com.intel.intelanalytics.security.UserPrincipal
import spray.json.JsObject

import scala.concurrent.ExecutionContext

/**
 * Provides context for an invocation of a command or query.
 *
 */
trait Invocation {
  /**
   * An instance of the engine that the plugin can use to execute its work
   */
  def engine: Engine

  /**
   * The identifier of this execution
   */
  def commandId: Long

  /**
   * The original arguments as supplied by the user
   */
  def arguments: Option[JsObject]

  /**
   * The user that invoked the operation
   */
  def user: UserPrincipal

  /**
   * A Scala execution context for use with methods that require one
   */
  def executionContext: ExecutionContext

  /**
   * Command Storage to read/update command progress
   */
  def commandStorage: CommandStorage
}
