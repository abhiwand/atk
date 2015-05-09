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

package com.intel.intelanalytics.engine

import com.intel.intelanalytics.domain.command.{ CommandTemplate, Command }
import scala.util.Try
import spray.json.JsObject

trait CommandStorage {
  def lookup(id: Long): Option[Command]
  def create(frame: CommandTemplate): Command
  def scan(offset: Int, count: Int): Seq[Command]
  def start(id: Long): Unit

  /**
   * On complete - mark progress as 100% or failed
   */
  def complete(id: Long, result: Try[JsObject]): Unit

  /* Stores result for the command but does not yet mark it complete */
  def storeResult(id: Long, result: Try[JsObject]): Unit

  /**
   * update command info regarding progress of jobs initiated by this command
   * @param id command id
   * @param progressInfo list of progress for the jobs initiated by this command
   */
  def updateProgress(id: Long, progressInfo: List[ProgressInfo]): Unit
}