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

import java.nio.file.Paths

import com.intel.intelanalytics.domain.query.{ Query, QueryTemplate }
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.engine.spark.{ SparkOps, HdfsFileStorage, SparkEngineConfig }
import com.intel.intelanalytics.engine.{ ProgressInfo, QueryStorage }
import com.intel.intelanalytics.repository.SlickMetaStoreComponent
import com.intel.intelanalytics.shared.EventLogging
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spray.json.JsObject

import scala.util.{ Failure, Success, Try }

class SparkQueryStorage(val metaStore: SlickMetaStoreComponent#SlickMetaStore, files: HdfsFileStorage) extends QueryStorage with EventLogging {
  val repo = metaStore.queryRepo

  /**
   * Create an RDD from a frame data file.
   * @param ctx spark context
   * @param queryId primary key of the query record
   * @return the newly created RDD
   */
  def getQueryRdd(ctx: SparkContext, queryId: Long): RDD[Row] = {
    ctx.objectFile(getAbsoluteFrameDirectory(queryId))
  }

  /**
   * Returns the data found in a single partition
   * @param ctx spark context
   * @param queryId primary key of the query record
   * @param partitionId partition number to return
   * @return data from partition as a local object
   */
  def getQueryPartition(ctx: SparkContext, queryId: Long, partitionId: Long): Iterable[Any] = {
    val rdd = getQueryRdd(ctx, queryId)
    //    rdd.mapPartitionsWithIndex((i, rows) => {
    //      if (i == partitionId) rows
    //      else Iterator.empty
    //    }).collect()
    val maxRows = SparkEngineConfig.maxRows
    SparkOps.getRows(rdd, partitionId * maxRows, maxRows, maxRows)
  }

  override def lookup(id: Long): Option[Query] =
    metaStore.withSession("se.query.lookup") {
      implicit session =>
        {}
        repo.lookup(id)
    }

  override def create(createQuery: QueryTemplate): Query =
    metaStore.withSession("se.query.create") {
      implicit session =>

        val created = repo.insert(createQuery)
        val query = repo.lookup(created.get.id).getOrElse(throw new Exception("Query not found immediately after creation"))
        drop(query.id)
        query
    }

  override def scan(offset: Int, count: Int): Seq[Query] = metaStore.withSession("se.query.getQuerys") {
    implicit session =>
      repo.scan(offset, count)
  }

  override def start(id: Long): Unit = {
    //TODO: set start date
  }

  override def complete(id: Long, totalPartitions: Try[Long]): Unit = {
    require(id > 0, "invalid ID")
    require(totalPartitions != null)
    metaStore.withSession("se.query.complete") {
      implicit session =>
        val query = repo.lookup(id).getOrElse(throw new IllegalArgumentException(s"Query $id not found"))
        if (query.complete) {
          warn(s"Ignoring completion attempt for query $id, already completed")
        }
        //TODO: Update dates
        import com.intel.intelanalytics.domain.throwableToError
        val changed = totalPartitions match {
          case Failure(ex) => query.copy(complete = true, error = Some(throwableToError(ex)))
          case Success(r) => {
            /**
             * update progress to 100 since the query is complete. This step is necessary
             * because the actually progress notification events are sent to SparkProgressListener.
             * The exact timing of the events arrival can not be determined.
             */

            val progress = query.progress.map(i => 100f)
            query.copy(complete = true, progress = progress, totalPartitions = Some(r))
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
  override def updateProgress(id: Long, progress: List[Float], detailedProgress: List[ProgressInfo]): Unit = {
    metaStore.withSession("se.query.updateProgress") {
      implicit session =>
        repo.updateProgress(id, progress, detailedProgress)
    }
  }

  val queryResultBase = "/intelanalytics/queryresults"

  def getFrameDirectory(id: Long): String = {
    val path = Paths.get(s"$queryResultBase/$id")
    path.toString
  }

  def getAbsoluteFrameDirectory(id: Long): String = {
    SparkEngineConfig.fsRoot + "/" + getFrameDirectory(id)
  }

  def drop(queryId: Long): Unit = withContext("frame.drop") {
    files.delete(Paths.get(getFrameDirectory(queryId)))
  }

}
