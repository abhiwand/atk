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

package org.apache.spark.mllib.ia.plugins.clustering

//Implicits needed for JSON conversion

import com.intel.intelanalytics.UnitReturn
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.engine.plugin.{ ApiMaturityTag, Invocation }
import org.apache.spark.frame.FrameRdd
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.mllib.clustering.{ KMeansModel, KMeans }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkContext._
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import org.apache.spark.mllib.ia.plugins.MLLibJsonProtocol._

class KMeansTrainPlugin extends SparkCommandPlugin[KMeansTrainArgs, KMeansTrainReturn] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:k_means/train"

  override def apiMaturityTag = Some(ApiMaturityTag.Beta)

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */

  /**
   * Number of Spark jobs that get created by running this command
   *
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: KMeansTrainArgs)(implicit invocation: Invocation) = 15
  /**
   * Run MLLib's LogisticRegressionWithSGD() on the training frame and create a Model for it.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   * as well as a function that can be called to produce a SparkContext that
   * can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: KMeansTrainArgs)(implicit invocation: Invocation): KMeansTrainReturn =
    {
      val models = engine.models
      val frames = engine.frames

      val inputFrame = frames.expectFrame(arguments.frame)

      //create RDD from the frame
      val trainFrameRdd = frames.loadFrameData(sc, inputFrame)

      /**
       * Constructs a KMeans instance with parameters passed or default parameters if not specified
       */
      val kMeans = initializeKmeans(arguments)

      val vectorRDD = trainFrameRdd.toDenseVectorRDDWithWeights(arguments.observationColumns, arguments.columnScalings)
      val kmeansModel = kMeans.run(vectorRDD)
      val size = computeClusterSize(kmeansModel, trainFrameRdd, arguments.observationColumns, arguments.columnScalings)
      val withinSetSumOfSquaredError = kmeansModel.computeCost(vectorRDD)

      //Writing the kmeansModel as JSON
      val jsonModel = new KMeansData(kmeansModel, arguments.observationColumns, arguments.columnScalings)
      val modelMeta = models.expectModel(arguments.model)
      models.updateModel(modelMeta.toReference, jsonModel.toJson.asJsObject)

      KMeansTrainReturn(size, withinSetSumOfSquaredError)
    }

  private def initializeKmeans(arguments: KMeansTrainArgs): KMeans = {
    val kmeans = new KMeans()

    kmeans.setK(arguments.getK)
    kmeans.setMaxIterations(arguments.getMaxIterations)
    kmeans.setInitializationMode(arguments.getInitializationMode)
    kmeans.setEpsilon(arguments.geteEpsilon)
  }

  private def computeClusterSize(kmeansModel: KMeansModel, trainFrameRdd: FrameRdd, observationColumns: List[String], columnScalings: List[Double]): Map[String, Int] = {

    val predictRDD = trainFrameRdd.mapRows(row => {
      val array = row.valuesAsArray(observationColumns).map(row => DataTypes.toDouble(row))
      val columnWeightsArray = columnScalings.toArray
      val doubles = array.zip(columnWeightsArray).map { case (x, y) => x * y }
      val point = Vectors.dense(doubles)
      kmeansModel.predict(point)
    })
    predictRDD.map(row => ("Cluster:" + (row + 1).toString, 1)).reduceByKey(_ + _).collect().toMap
  }
}
