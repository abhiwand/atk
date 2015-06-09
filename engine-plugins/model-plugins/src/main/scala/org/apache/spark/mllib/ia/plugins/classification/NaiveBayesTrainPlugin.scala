//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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

package org.apache.spark.mllib.ia.plugins.classification

import com.intel.intelanalytics.UnitReturn
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.FrameReference
import com.intel.intelanalytics.domain.model.ModelReference

import com.intel.intelanalytics.engine.plugin.{ ApiMaturityTag, Invocation }
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.classification.{ NaiveBayes, NaiveBayesModel }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import org.apache.spark.mllib.ia.plugins.MLLibJsonProtocol._

//Implicits needed for JSON conversion
import spray.json._

case class NaiveBayesTrainArgs(model: ModelReference,
                               frame: FrameReference,
                               labelColumn: String,
                               observationColumns: List[String],
                               lambdaParameter: Option[Double] = None) {
  require(model != null, "model is required")
  require(frame != null, "frame is required")
  require(observationColumns != null && !observationColumns.isEmpty, "observationColumn must not be null nor empty")
  require(labelColumn != null && !labelColumn.isEmpty, "labelColumn must not be null nor empty")
}

class NaiveBayesTrainPlugin extends SparkCommandPlugin[NaiveBayesTrainArgs, UnitReturn] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:naive_bayes/train"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: NaiveBayesTrainArgs)(implicit invocation: Invocation) = 109

  /**
   * Run MLLib's NaiveBayes() on the training frame and create a Model for it.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: NaiveBayesTrainArgs)(implicit invocation: Invocation): UnitReturn = {
    val models = engine.models
    val frames = engine.frames

    val inputFrame = frames.expectFrame(arguments.frame)
    val modelMeta = models.expectModel(arguments.model)

    //create RDD from the frame
    val trainFrameRdd = frames.loadFrameData(sc, inputFrame)
    val labeledTrainRdd: RDD[LabeledPoint] = trainFrameRdd.toLabeledPointRDD(arguments.labelColumn, arguments.observationColumns)

    //Running MLLib
    val naiveBayes = new NaiveBayes()
    naiveBayes.setLambda(arguments.lambdaParameter.getOrElse(1.0))

    val naiveBayesModel = naiveBayes.run(labeledTrainRdd)
    val jsonModel = new NaiveBayesData(naiveBayesModel, arguments.observationColumns)

    //TODO: Call save instead once implemented for models
    models.updateModel(modelMeta.toReference, jsonModel.toJson.asJsObject)
    new UnitReturn
  }
}