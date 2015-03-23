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

package com.intel.intelanalytics.engine.plugin

import com.intel.event.EventContext
import com.intel.intelanalytics.engine.{ ReferenceResolver, CommandStorage, Engine }
import com.intel.intelanalytics.security.UserPrincipal
import spray.json.JsObject

import scala.concurrent.ExecutionContext

import Invocation._
import com.intel.intelanalytics.engine.spark.CommandProgressUpdater

/**
 * Provides context for an invocation of a command or query.
 *
 */
trait Invocation {
  /**
   * The user that invoked the operation
   */
  private[intelanalytics] def user: UserPrincipal

  /**
   * A Scala execution context for use with methods that require one
   */
  private[intelanalytics] def executionContext: ExecutionContext

  /**
   * Reference resolver to enable de-referencing of UriReference objects
   */
  private[intelanalytics] def resolver: ReferenceResolver

  /**
   * EventContext of the caller
   */
  private[intelanalytics] def eventContext: EventContext

  /**
   * Update the progress
   * @param progress current progress
   */
  private[intelanalytics] def updateProgress(progress: Float): Unit = ???
}

/**
 * Invocation class to be used by Engine when performing operations by scheduled tasks
 * @param executionContext A Scala execution context for use with methods that require one
 * @param resolver Reference resolver to enable de-referencing of UriReference objects
 */
case class BackendInvocation(executionContext: ExecutionContext = ExecutionContext.Implicits.global,
                             resolver: ReferenceResolver = ReferenceResolver) extends Invocation {
  private[intelanalytics] def user: UserPrincipal = null
  private[intelanalytics] def eventContext: EventContext = null
}

case class Call(user: UserPrincipal,
                executionContext: ExecutionContext = ExecutionContext.Implicits.global,
                resolver: ReferenceResolver = ReferenceResolver,
                eventContext: EventContext = null) extends Invocation

trait CommandInvocation extends Invocation {
  /**
   * An instance of the engine that the plugin can use to execute its work
   */
  private[intelanalytics] def engine: Engine

  /**
   * The identifier of this execution
   */
  private[intelanalytics] def commandId: Long

  /**
   * The original arguments as supplied by the user
   */
  private[intelanalytics] def arguments: Option[JsObject]

  /**
   * Command Storage to read/update command progress
   */
  private[intelanalytics] def commandStorage: CommandStorage

  val progressUpdater: CommandProgressUpdater

  override private[intelanalytics] def updateProgress(progress: Float): Unit = progressUpdater.updateProgress(commandId, progress)
}

object Invocation {
  implicit def invocationToUser(implicit inv: Invocation): UserPrincipal = inv.user
  implicit def invocationToEventContext(implicit inv: Invocation): EventContext = inv.eventContext
}
