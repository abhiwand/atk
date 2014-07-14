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
package com.intel.intelanalytics.service.v1.decorators

import com.intel.intelanalytics.domain.query.Query
import com.intel.intelanalytics.service.v1.viewmodels._
import spray.json.JsValue

import scala.collection.mutable.ListBuffer

/**
 * A decorator that takes an entity from the database and converts it to a View/Model
 * for delivering via REST services
 */
object QueryDecorator extends EntityDecorator[Query, GetQueries, GetQuery] {

  /**
   * Decorate a single entity (like you would want in "GET /entities/id")
   *
   * @param uri UNUSED? DELETE?
   * @param links related links
   * @param entity the entity to decorate
   * @return the View/Model
   */
  override def decorateEntity(uri: String, links: Iterable[RelLink], entity: Query): GetQuery = {
    val detailedProgressMessage = entity.detailedProgress.map(progress => progress.toString)

    GetQuery(id = entity.id, name = entity.name,
      arguments = entity.arguments, error = entity.error, progress = entity.progress, progressMessage = detailedProgressMessage, complete = entity.complete,
      result = if (entity.complete) {
        Some(GetQueryPartition(None, None, entity.totalPartitions))
      }
      else {
        None
      }, links = links.toList)
  }

  /**
   * Decorate a list of entities (like you would want in "GET /entities")
   *
   * @param uri the base URI, for this type of entity "../entities"
   * @param entities the list of entities to decorate
   * @return the View/Model
   */
  override def decorateForIndex(uri: String, entities: Seq[Query]): List[GetQueries] = {
    entities.map(query => new GetQueries(id = query.id,
      name = query.name,
      url = uri + "/" + query.id)).toList
  }

  /**
   * Decorate a list of data partitions There will be one record per partition of the query RDD
   *
   * @param uri the base URI, for this type query ie ../queries/id
   * @param entity query to display partitions
   * @return the View/Model
   */
  def decoratePartitions(uri: String, entity: Query): List[GetQueryPartitions] = {
    require(entity.complete)
    val partitions = new ListBuffer[GetQueryPartitions]();
    for (i <- 1 to entity.totalPartitions.get.toInt) {
      partitions += new GetQueryPartitions(id = i, url = uri + "/" + i)
    }
    partitions.toList
  }

  /**
   * Decorate a retrieved data partition
   *
   * @param uri uri of the query
   * @param links related links
   * @param entity query retrieved
   * @param partition partition requested
   * @param data data found in the partitiion as a List of JsValues
   * @return the View/Model
   */
  def decoratePartition(uri: String, links: Iterable[RelLink], entity: Query, partition: Long, data: List[JsValue]): GetQuery = {
    require(entity.complete)
    val detailedProgressMessage = entity.detailedProgress.map(progress => progress.toString)

    GetQuery(id = entity.id, name = entity.name,
      arguments = entity.arguments, error = entity.error, progress = entity.progress, progressMessage = detailedProgressMessage, complete = entity.complete,
      result = Some(new GetQueryPartition(Some(data), Some(partition), entity.totalPartitions)), links = links.toList)
  }
}