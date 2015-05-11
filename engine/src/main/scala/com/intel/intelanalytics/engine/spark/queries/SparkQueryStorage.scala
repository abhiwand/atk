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

package com.intel.intelanalytics.engine.spark.queries

import java.nio.file.Paths

import com.intel.intelanalytics.EventLoggingImplicits
import com.intel.intelanalytics.domain.query.{ Query, QueryTemplate }
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.engine.plugin.{ Invocation, QueryPluginResults }
import com.intel.intelanalytics.engine.spark.{ HdfsFileStorage, SparkEngineConfig }
import com.intel.intelanalytics.engine.{ ProgressInfo, QueryStorage }
import com.intel.intelanalytics.repository.SlickMetaStoreComponent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spray.json.JsObject

import scala.util.{ Failure, Success, Try }
import org.apache.hadoop.fs.Path
import com.intel.intelanalytics.engine.spark.frame.MiscFrameFunctions
import com.intel.event.EventLogging

/**
 * Class Responsible for coordinating Query Storage with Spark
 * @param metaStore metastore to save DB related info
 * @param files  Object for saving FileSystem related Data
 */
class SparkQueryStorage(val metaStore: SlickMetaStoreComponent#SlickMetaStore, files: HdfsFileStorage)
    extends QueryStorage
    with EventLogging
    with EventLoggingImplicits {
  val repo = metaStore.queryRepo

  /**
   * Create an RDD from a frame data file.
   * @param sc spark context
   * @param queryId primary key of the query record
   * @return the newly created RDD
   */
  def getQueryRdd(sc: SparkContext, queryId: Long): RDD[Array[Any]] = {
    sc.objectFile(getAbsoluteQueryDirectory(queryId))
  }

  /**
   * Returns the data found in a single partition
   * @param sc spark context
   * @param queryId primary key of the query record
   * @param pageId partition number to return
   * @return data from partition as a local object
   */
  def getQueryPage(sc: SparkContext, queryId: Long, pageId: Long)(implicit invocation: Invocation): Iterable[Array[Any]] = {
    val rdd = getQueryRdd(sc, queryId)
    val query = lookup(queryId)
    val (pageSize: Int, totalPages: Int) = query match {
      case Some(q) => (
        q.pageSize.getOrElse(SparkEngineConfig.pageSize.toLong).toInt,
        q.totalPages.getOrElse(-1l).toInt)
      case None => (SparkEngineConfig.pageSize, -1)
    }
    if (totalPages == 1)
      rdd.collect()
    else
      MiscFrameFunctions.getRows[Array[Any]](rdd, pageId * pageSize, pageSize, pageSize)
  }

  /**
   * Retrieve a query from the database
   * @param id  query_id
   * @return The specified Query
   */
  override def lookup(id: Long)(implicit invocation: Invocation): Option[Query] =
    metaStore.withSession("se.query.lookup") {
      implicit session =>
        {}
        repo.lookup(id)
    }

  /**
   * Create a new Query based off of the existing template
   * @param createQuery Template describing the new query
   * @return the new Query
   */
  override def create(createQuery: QueryTemplate)(implicit invocation: Invocation): Query =
    metaStore.withSession("se.query.create") {
      implicit session =>

        val created = repo.insert(createQuery)
        val query = repo.lookup(created.get.id).getOrElse(throw new Exception("Query not found immediately after creation"))
        dropFiles(query.id) //delete any old query info in case of file system conflicts
        query
    }

  /**
   * Select multiple queries from the db
   * @param offset offset to begin from
   * @param count number of queries to return
   * @return the requested number of query
   */
  override def scan(offset: Int, count: Int)(implicit invocation: Invocation): Seq[Query] = metaStore.withSession("se.query.getQueries") {
    implicit session =>
      repo.scan(offset, count)
  }

  override def start(id: Long)(implicit invocation: Invocation): Unit = {
    //TODO: set start date
  }

  /**
   * Complete a query object. To be executed after execution
   * @param id Query ID
   * @param result Object Describing Query Result
   */
  override def complete(id: Long, result: Try[JsObject])(implicit invocation: Invocation): Unit = {
    require(id > 0, "invalid ID")
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

            import com.intel.intelanalytics.domain.DomainJsonProtocol._

            val pluginResults = Some(r).get.convertTo[QueryPluginResults]
            query.copy(complete = true, totalPages = Some(pluginResults.totalPages), pageSize = Some(pluginResults.pageSize))
          }
        }
        repo.update(changed)
    }
  }

  val queryResultBase = "/intelanalytics/queryresults"

  /**
   * Retrieve the directory that the Query RDDs should be stored in
   * @param id query id
   * @return directory path
   */
  def getQueryDirectory(id: Long): String = {
    val path = Paths.get(s"$queryResultBase/$id")
    path.toString
  }

  /**
   * Retrieve the absolute directory path that the Query RDDs should be stored in
   * @param id query id
   * @return absolute directory path
   */
  def getAbsoluteQueryDirectory(id: Long): String = {
    SparkEngineConfig.fsRoot + "/" + getQueryDirectory(id)
  }

  /**
   * remove existing queries files
   * @param queryId
   */
  def dropFiles(queryId: Long)(implicit invocation: Invocation): Unit = withContext("frame.drop") {
    files.delete(new Path(getAbsoluteQueryDirectory(queryId)))
  }

}
