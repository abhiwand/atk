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

package com.intel.taproot.analytics.engine

import com.intel.taproot.analytics.domain.model.{ ModelReference, ModelTemplate, ModelEntity }
import com.intel.taproot.analytics.engine.plugin.Invocation
import com.intel.taproot.analytics.security.UserPrincipal
import spray.json.{ JsValue, JsObject }
import com.intel.taproot.analytics.domain.CreateEntityArgs

trait ModelStorage {

  def expectModel(modelRef: ModelReference): ModelEntity

  def createModel(model: CreateEntityArgs)(implicit invocation: Invocation): ModelEntity

  def renameModel(modelRef: ModelReference, newName: String): ModelEntity

  def drop(modelRef: ModelReference)

  def getModels()(implicit invocation: Invocation): Seq[ModelEntity]

  def getModelByName(name: Option[String]): Option[ModelEntity]

  def updateModel(modelReference: ModelReference, newData: JsObject)(implicit invocation: Invocation): ModelEntity

  def scheduleDeletion(model: ModelEntity)(implicit invocation: Invocation): Unit

}