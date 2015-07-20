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

package com.intel.taproot.analytics.engine.model

import com.intel.taproot.analytics.domain.model.{ ModelEntity, ModelReference }
import com.intel.taproot.analytics.engine.ModelStorage
import spray.json.JsObject

/**
 * Model interface for plugin authors
 */
trait Model {

  @deprecated("it is better if plugin authors do not have direct access to entity")
  def entity: ModelEntity

  def name: Option[String]

  def modelType: String

  def description: Option[String]

  def statusId: Long

  /**
   * Expects model has been trained and data exists, throws appropriate exception otherwise
   */
  def data: JsObject

  def dataOption: Option[JsObject]
}

object Model {

  implicit def modelToModelReference(model: Model): ModelReference = model.entity.toReference

  implicit def modelToModelEntity(model: Model): ModelEntity = model.entity

}

class ModelImpl(modelRef: ModelReference, modelStorage: ModelStorage) extends Model {

  override def entity: ModelEntity = modelStorage.expectModel(modelRef)

  override def name: Option[String] = entity.name

  override def modelType: String = entity.modelType

  override def description: Option[String] = entity.description

  override def statusId: Long = entity.statusId

  /**
   * Expects model has been trained and data exists, throws appropriate exception otherwise
   */
  override def data: JsObject = entity.data.getOrElse(throw new RuntimeException("Model has not yet been trained"))

  override def dataOption: Option[JsObject] = entity.data

}
