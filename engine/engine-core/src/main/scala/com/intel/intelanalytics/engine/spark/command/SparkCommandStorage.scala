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

package com.intel.intelanalytics.engine.spark.command

import com.intel.intelanalytics.domain.Error
import com.jcraft.jsch.Session
import org.joda.time.DateTime
import scala.util.{ Success, Failure, Try }
import spray.json.JsObject
import com.intel.intelanalytics.domain.command.{ CommandTemplate, Command }
import com.intel.intelanalytics.engine.{ ProgressInfo, TaskProgressInfo, CommandStorage }
import com.intel.intelanalytics.repository.{ SlickMetaStoreComponent }
import com.intel.event.{ EventContext, EventLogging }

class SparkCommandStorage(val metaStore: SlickMetaStoreComponent#SlickMetaStore) extends CommandStorage with EventLogging {
  val repo = metaStore.commandRepo

  override def lookup(id: Long): Option[Command] =
    metaStore.withSession("se.command.lookup") {
      implicit session =>
        repo.lookup(id)
    }

  override def create(createReq: CommandTemplate): Command =
    metaStore.withSession("se.command.create") {
      implicit session =>

        val created = repo.insert(createReq)
        repo.lookup(created.get.id).getOrElse(throw new Exception("Command not found immediately after creation"))
    }

  override def scan(offset: Int, count: Int): Seq[Command] = metaStore.withSession("se.command.getCommands") {
    implicit session =>
      repo.scan(offset, count).sortBy(c => c.id) //TODO: Can't seem to get db to produce sorted results.
  }

  override def start(id: Long): Unit = {
    //TODO: set start date
  }

  /**
   * On complete - mark progress as 100% or failed
   */
  override def complete(id: Long, result: Try[JsObject]): Unit = {
    require(id > 0, "invalid ID")
    require(result != null, "result must not be null")
    updateResult(id, result, true)
  }

  override def storeResult(id: Long, result: Try[JsObject]): Unit = {
    require(id > 0, "invalid ID")
    require(result != null, "result must not be null")
    updateResult(id, result, false)
  }

  private def updateResult(id: Long, result: Try[JsObject], markComplete: Boolean): Unit = {
    metaStore.withSession("se.command.updateResult") {
      implicit session =>
        val command = repo.lookup(id).getOrElse(throw new IllegalArgumentException(s"Command $id not found"))
        val corId = EventContext.getCurrent.getCorrelationId
        if (command.complete) {
          warn(s"Ignoring completion attempt for command $id, already completed")
        }
        import com.intel.intelanalytics.domain.throwableToError
        val changed = result match {
          case Failure(ex) =>
            error(s"command completed with error, id: $id, name: ${command.name}, args: ${command.compactArgs} ", exception = ex)

            command.copy(complete = markComplete,
              error = Some(throwableToError(ex)),
              correlationId = corId)
          case Success(r) => {
            // update progress to 100 since the command is complete. This step is necessary
            // because the actually progress notification events are sent to SparkProgressListener.
            // The exact timing of the events arrival can not be determined.
            val progress = command.progress.map(info => info.copy(progress = 100f))
            command.copy(complete = markComplete,
              progress = if (progress.nonEmpty) progress else List(ProgressInfo(100f, None)),
              result = Some(r),
              error = None,
              correlationId = corId)
          }
        }
        repo.update(changed)
    }
  }

  /**
   * update progress information for the command
   * @param id command id
   * @param progressInfo progress
   */
  override def updateProgress(id: Long, progressInfo: List[ProgressInfo]): Unit = {
    metaStore.withSession("se.command.updateProgress") {
      implicit session =>
        repo.updateProgress(id, progressInfo)
    }
  }
}
