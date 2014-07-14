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

import com.intel.intelanalytics.domain.frame.DataFrame
import com.intel.intelanalytics.service.v1.viewmodels.{ Rel, RelLink, GetDataFrame, GetDataFrames }
import spray.http.Uri
import org.apache.commons.lang.StringUtils

/**
 * A decorator that takes an entity from the database and converts it to a View/Model
 * for delivering via REST services
 */
object FrameDecorator extends EntityDecorator[DataFrame, GetDataFrames, GetDataFrame] {

  /**
   * Decorate a single entity (like you would want in "GET /entities/id")
   *
   * Self-link and errorFrame links are auto-created from supplied uri.
   *
   * @param uri the uri to the current entity
   * @param additionalLinks related links
   * @param entity the entity to decorate
   * @return the View/Model
   */
  override def decorateEntity(uri: String, additionalLinks: Iterable[RelLink] = Nil, entity: DataFrame): GetDataFrame = {

    var links = List(Rel.self(uri.toString)) ++ additionalLinks

    if (entity.errorFrameId.isDefined) {
      val baseUri = StringUtils.substringBeforeLast(uri, "/")
      links = RelLink("errorFrame", baseUri + "/" + entity.errorFrameId.get, "GET") :: links
    }

    GetDataFrame(id = entity.id, name = entity.name, schema = entity.schema, links)
  }

  /**
   * Decorate a list of entities (like you would want in "GET /entities")
   *
   * @param uri the base URI, for this type of entity "../entities"
   * @param entities the list of entities to decorate
   * @return the View/Model
   */
  override def decorateForIndex(uri: String, entities: Seq[DataFrame]): List[GetDataFrames] = {
    entities.map(frame => new GetDataFrames(id = frame.id,
      name = frame.name,
      url = uri + "/" + frame.id)).toList
  }
}