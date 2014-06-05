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

package com.intel.intelanalytics.engine.spark.command

import com.intel.intelanalytics.engine.CommandComponent.CommandStorage
import com.intel.intelanalytics.domain.Error
import scala.util.{Success, Failure, Try}
import spray.json.JsObject
import com.intel.intelanalytics.domain.command.{CommandTemplate, Command}

trait SparkCommandStorage extends CommandStorage {
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
      repo.scan(offset, count)
  }

  override def start(id: Long): Unit = {
    //TODO: set start date
  }

  override def complete(id: Long, result: Try[JsObject]): Unit = {
    require(id > 0, "invalid ID")
    require(result != null)
    metaStore.withSession("se.command.complete") {
      implicit session =>
        val command = repo.lookup(id).getOrElse(throw new IllegalArgumentException(s"Command $id not found"))
        if (command.complete) {
          warn(s"Ignoring completion attempt for command $id, already completed")
        }
        //TODO: Update dates
        val changed = result match {
          case Failure(ex) => command.copy(complete = true, error = Some(ex: Error))
          case Success(r) => command.copy(complete = true, result = Some(r))
        }
        repo.update(changed)
    }
  }

}
