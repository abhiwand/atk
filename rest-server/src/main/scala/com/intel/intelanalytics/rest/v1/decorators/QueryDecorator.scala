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

package com.intel.intelanalytics.rest.v1.decorators

import com.intel.intelanalytics.domain.query.{ QueryDataResult, Query }
import com.intel.intelanalytics.rest.v1.viewmodels._
import spray.json.{ JsNull, JsValue }

import scala.collection.mutable.ListBuffer
import com.intel.intelanalytics.domain.schema.Schema

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
    decorateEntity(uri, links, entity, None)
  }

  /**
   * Decorate a single entity (like you would want in "GET /entities/id")
   *
   * @param uri UNUSED? DELETE?
   * @param links related links
   * @param entity the entity to decorate
   * @param schema schema to describe the data returned by query
   * @return the View/Model
   */
  def decorateEntity(uri: String, links: Iterable[RelLink], entity: Query, schema: Option[Schema]): GetQuery = {
    GetQuery(id = Some(entity.id), name = entity.name,
      arguments = entity.arguments, error = entity.error, complete = entity.complete,
      result = if (entity.complete) {
        Some(GetQueryPage(None, None, entity.totalPages, schema))
      }
      else {
        schema match {
          case Some(_) => Some(GetQueryPage(None, None, None, schema = schema))
          case _ => None
        }
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
  def decoratePages(uri: String, entity: Query): List[GetQueryPages] = {
    require(entity.complete)
    val pages = new ListBuffer[GetQueryPages]()
    for (i <- 1 to entity.totalPages.get.toInt) {
      pages += new GetQueryPages(id = i, url = uri + "/" + i)
    }
    pages.toList
  }

  /**
   * Decorate a retrieved data partition
   *
   * @param uri uri of the query
   * @param links related links
   * @param entity query retrieved
   * @param page page requested
   * @param data data found in the partitiion as a List of JsValues
   * @param schema schema to describe the data returned by query
   * @return the View/Model
   */
  def decoratePage(uri: String, links: Iterable[RelLink], entity: Query, page: Long, data: List[JsValue], schema: Option[Schema]): GetQuery = {
    require(entity.complete)

    GetQuery(id = Some(entity.id), name = entity.name,
      arguments = entity.arguments, error = entity.error, complete = entity.complete,
      result = Some(new GetQueryPage(Some(data), Some(page), entity.totalPages, schema)), links = links.toList)
  }

}
