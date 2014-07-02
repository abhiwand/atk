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

package com.intel.intelanalytics.engine.spark.queries

import com.intel.intelanalytics.domain.query.{Query, QueryTemplate}
import com.intel.intelanalytics.engine.{QueryStorage}
import com.intel.intelanalytics.repository.SlickMetaStoreComponent
import com.intel.intelanalytics.shared.EventLogging
import spray.json.JsObject

import scala.util.{Failure, Success, Try}

class SparkQueryStorage(val metaStore: SlickMetaStoreComponent#SlickMetaStore) extends QueryStorage with EventLogging {
  val repo = metaStore.queryRepo

  override def lookup(id: Long): Option[Query] =
    metaStore.withSession("se.query.lookup") {
      implicit session =>
        repo.lookup(id)
    }

  override def create(createReq: QueryTemplate): Query =
    metaStore.withSession("se.query.create") {
      implicit session =>

        val created = repo.insert(createReq)
        repo.lookup(created.get.id).getOrElse(throw new Exception("Query not found immediately after creation"))
    }

  override def scan(offset: Int, count: Int): Seq[Query] = metaStore.withSession("se.query.getQuerys") {
    implicit session =>
      repo.scan(offset, count)
  }

  override def start(id: Long): Unit = {
    //TODO: set start date
  }

  override def complete(id: Long, result: Try[JsObject]): Unit = {
    require(id > 0, "invalid ID")
    require(result != null)
    metaStore.withSession("se.query.complete") {
      implicit session =>
        val query = repo.lookup(id).getOrElse(throw new IllegalArgumentException(s"Query $id not found"))
        if (query.complete) {
          warn(s"Ignoring completion attempt for query $id, already completed")
        }
        //TODO: Update dates
        import com.intel.intelanalytics.domain.throwableToError
        val changed = result match {
          case Failure(ex) => query.copy(complete = true, error = Some(throwableToError(ex)))
          case Success(r) => {
            /**
             * update progress to 100 since the query is complete. This step is necessary
             * because the actually progress notification events are sent to SparkProgressListener.
             * The exact timing of the events arrival can not be determined.
             */

            val progress = query.progress.map(i => 100f)
            query.copy(complete = true, progress = progress, result = Some(r))
          }
        }
        repo.update(changed)
    }
  }

  /**
   * update progress information for the query
   * @param id query id
   * @param progress progress
   */
  override def updateProgress(id: Long, progress: List[Float]): Unit = {
    metaStore.withSession("se.query.updateProgress") {
      implicit session =>
        repo.updateProgress(id, progress)
    }
  }
}
