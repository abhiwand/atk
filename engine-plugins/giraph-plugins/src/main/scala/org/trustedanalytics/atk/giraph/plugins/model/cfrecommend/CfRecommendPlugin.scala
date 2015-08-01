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

package org.trustedanalytics.atk.giraph.plugins.model.cfrecommend

import org.trustedanalytics.atk.giraph.config.cfrecommend._
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.schema.{ Column, DataTypes, FrameSchema }
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, CommandPlugin, Invocation, PluginDoc }
import org.trustedanalytics.atk.giraph.config.lda.LdaJsonFormat
import spray.json._
import CfRecommendJsonFormat._

/**
 * Collaborative filtering recommend model
 */
@PluginDoc(oneLine = "",
  extended = "",
  returns = "")
class CfRecommendPlugin
    extends CommandPlugin[CfRecommendArgs, CfRecommendResult] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:cf/recommend"

  override def apiMaturityTag = Some(ApiMaturityTag.Beta)

  override def execute(arguments: CfRecommendArgs)(implicit invocation: Invocation): CfRecommendResult = {

    val frames = engine.frames

    // validate arguments
    val userFrame = frames.expectFrame(arguments.userFrame)
    userFrame.schema.requireColumnIsType(arguments.userColumnName, DataTypes.string)
    userFrame.schema.requireColumnIsType(arguments.cfFactorsColumnName, DataTypes.vector(arguments.numFactors))
    require(userFrame.isParquet, "frame must be stored as parquet file, or support for new input format is needed")

    val itemFrame = frames.expectFrame(arguments.itemFrame)
    itemFrame.schema.requireColumnIsType(arguments.userColumnName, DataTypes.string)
    itemFrame.schema.requireColumnIsType(arguments.cfFactorsColumnName, DataTypes.vector(arguments.numFactors))
    require(itemFrame.isParquet, "frame must be stored as parquet file, or support for new input format is needed")

    // setup
    val cfRecommendOutputFrame = frames.prepareForSave(CreateEntityArgs(description = Some("CF recommend results")))
    val inputFormatConfig = new CfRecommendInputFormatConfig(userFrame.getStorageLocation, itemFrame.getStorageLocation, userFrame.schema)
    val outputFormatConfig = new CfRecommendOutputFormatConfig(cfRecommendOutputFrame.getStorageLocation)

    //
    // add algorithm here
    //

    val resultsColumn = Column("cf_recommend", DataTypes.int64)
    frames.postSave(None, cfRecommendOutputFrame.toReference, new FrameSchema(List(itemFrame.schema.column(arguments.itemColumnName), resultsColumn)))

    CfRecommendResult(frames.expectFrame(cfRecommendOutputFrame.toReference))
  }

}
