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

package org.apache.spark.mllib.ia.plugins.clustering

//Implicits needed for JSON conversion

import com.intel.intelanalytics.UnitReturn
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.model.KMeansTrainArgs
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import org.apache.spark.mllib.ia.plugins.MLLibJsonProtocol._

class KMeansTrainPlugin extends SparkCommandPlugin[KMeansTrainArgs, UnitReturn] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:k_means/train"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */

  override def doc: Option[CommandDoc] = Some(CommandDoc(oneLineSummary = "Creating a Kmeans Model using the observation column of a train frame",
    extendedSummary = Some(
      """
                             |
                             |    Parameters
                             |    ----------
                             |    frame: Frame
                             |        Frame to train the model on
                             |
                             |    observation_column: str
                             |        column containing the observations
                             |
                             |
                             |    Returns
                             |    -------
                             |    Trained Kmeans model object
                             |
                             |
                             |    Examples
                             |    --------
                             |    ::
                             |
                             |        model = ia.KmeansModel(name='MyKmeansModel')
                             |        model.train(train_frame, ['name_of_observation_column1','name_of_observation_column2'])
                             |
                             |
                           """.stripMargin)))

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: KMeansTrainArgs)(implicit invocation: Invocation) = 1
  /**
   * Run MLLib's LogisticRegressionWithSGD() on the training frame and create a Model for it.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: KMeansTrainArgs)(implicit invocation: Invocation): UnitReturn =
    {
      val models = engine.models
      val frames = engine.frames

      val inputFrame = frames.expectFrame(arguments.frame.id)
      val modelMeta = models.expectModel(arguments.model.id)

      //create RDD from the frame
      val trainFrameRDD = frames.loadFrameData(sc, inputFrame)

      /**
       * Constructs a KMeans instance with parameters passed or default parameters if not specified
       */
      val kMeans = initializeKmeans(arguments)

      /*TODO: Add to FrameRDD */
      val vectorRDD = trainFrameRDD.mapRows(row => {
        val array = row.valuesAsArray(arguments.observationColumns)
        val doubles = array.map(i => DataTypes.toDouble(i))
        Vectors.dense(doubles)
      })

      val kmeansModel = kMeans.run(vectorRDD)
      val jsonModel = kmeansModel.toJson.asJsObject

      models.updateModel(modelMeta, jsonModel)
      new UnitReturn

    }

  private def initializeKmeans(arguments: KMeansTrainArgs): KMeans = {
    val kmeans = new KMeans()
    if (arguments.k.isDefined) { kmeans.setK(arguments.k.get) }
    if (arguments.maxIterations.isDefined) { kmeans.setMaxIterations(arguments.maxIterations.get) }
    if (arguments.epsilon.isDefined) { kmeans.setEpsilon(arguments.epsilon.get) }
    if (arguments.initializationMode.isDefined) { kmeans.setInitializationMode(arguments.initializationMode.get) }
    kmeans
  }
}
