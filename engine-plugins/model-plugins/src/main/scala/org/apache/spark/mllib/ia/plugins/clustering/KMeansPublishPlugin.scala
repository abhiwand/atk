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

import org.apache.spark.mllib.atk.plugins.clustering.KMeansData
import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.component.Archive
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.domain.{ CreateEntityArgs, Naming }
import org.trustedanalytics.atk.domain.frame._
import org.trustedanalytics.atk.domain.schema.Column
import org.trustedanalytics.atk.domain.schema.{ FrameSchema, DataTypes }
import org.trustedanalytics.atk.domain.schema.DataTypes._
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin._
import org.trustedanalytics.atk.engine.model.plugins.scoring.{ ModelPublish, ModelPublishArgs }
import scala.collection.mutable.ListBuffer
import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol._
// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Rename columns of a frame
 */
@PluginDoc(oneLine = "",
  extended = "",
  returns = "")
class KMeansPublishPlugin extends CommandPlugin[ModelPublishArgs, UnitReturn] {

  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:k_means/publish"

  override def apiMaturityTag = Some(ApiMaturityTag.Beta)

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */

  override def numberOfJobs(arguments: ModelPublishArgs)(implicit invocation: Invocation) = 1

  /**
   * Get the predictions for observations in a test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: ModelPublishArgs)(implicit invocation: Invocation): UnitReturn = {

    val model: Model = arguments.model

    //Extracting the KMeansModel from the stored JsObject
    val kmeansData = model.data.convertTo[KMeansData]
    val kmeansModel = kmeansData.kMeansModel
    val jsvalue: JsValue = kmeansModel.toJson

    ModelPublish.createTarForScoringEngine(jsvalue.toString(), arguments.serviceName, "scoring-models", arguments.filePath, "org.trustedanalytics.atk.scoring.models.LibKMeansModelReaderPlugin")

  }
}
