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

package org.apache.spark.mllib.ia.plugins.clustering

import com.intel.intelanalytics.domain.CreateEntityArgs
import com.intel.intelanalytics.domain.model.{ KMeansNewArgs, ModelEntity }
import com.intel.intelanalytics.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin

//Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

@PluginDoc(oneLine = "",
  extended = "")
class KMeansNewPlugin extends SparkCommandPlugin[KMeansNewArgs, ModelEntity] {

  override def name: String = "model:k_means/new"

  override def execute(arguments: KMeansNewArgs)(implicit invocation: Invocation): ModelEntity =
    {
      val models = engine.models
      models.createModel(CreateEntityArgs(name = arguments.name, entityType = Some("model:k_means")))
    }
}
