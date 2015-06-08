/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

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
