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

package com.intel.taproot.analytics.algorithm.model

import com.intel.taproot.analytics.domain.CreateEntityArgs
import com.intel.taproot.analytics.domain.model.{ GenericNewModelArgs, ModelEntity }
import com.intel.taproot.analytics.engine.plugin.{ CommandPlugin, Invocation, PluginDoc }

import spray.json._
import com.intel.taproot.analytics.domain.DomainJsonProtocol._

/**
 * Latent Dirichlet allocation - create a 'new' instance of this model
 */
@PluginDoc(oneLine = "<TBD>",
  extended = """<TBD>""")
class LdaNewPlugin extends CommandPlugin[GenericNewModelArgs, ModelEntity] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:lda/new"

  override def execute(arguments: GenericNewModelArgs)(implicit invocation: Invocation): ModelEntity = {
    val models = engine.models
    models.createModel(CreateEntityArgs(name = arguments.name, entityType = Some("model:lda")))
  }
}
