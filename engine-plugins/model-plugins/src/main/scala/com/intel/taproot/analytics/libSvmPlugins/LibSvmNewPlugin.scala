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

package com.intel.taproot.analytics.libSvmPlugins

import com.intel.taproot.analytics.domain.CreateEntityArgs
import com.intel.taproot.analytics.domain.model.{ GenericNewModelArgs, ModelEntity }
import com.intel.taproot.analytics.engine.plugin.{ ApiMaturityTag, ArgDoc, Invocation, PluginDoc }
import com.intel.taproot.analytics.engine.plugin.SparkCommandPlugin
import com.intel.taproot.analytics.domain.DomainJsonProtocol._

@PluginDoc(oneLine = "model:libsvm/new",
  extended = "")
class LibSvmNewPlugin extends SparkCommandPlugin[GenericNewModelArgs, ModelEntity] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:libsvm/new"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  override def execute(arguments: GenericNewModelArgs)(implicit invocation: Invocation): ModelEntity = {
    engine.models.createModel(CreateEntityArgs(name = arguments.name, entityType = Some("model:libsvm")))
  }
}