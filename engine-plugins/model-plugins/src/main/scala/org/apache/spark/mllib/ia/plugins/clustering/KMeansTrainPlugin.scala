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

//Implicits needed for JSON conversion

import com.intel.intelanalytics.UnitReturn
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.engine.plugin.{ ApiMaturityTag, ArgDoc, Invocation, PluginDoc }
import org.apache.spark.frame.FrameRdd
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.mllib.clustering.{ KMeansModel, KMeans }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkContext._
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import org.apache.spark.mllib.ia.plugins.MLLibJsonProtocol._

/**
 * Parameters
 * ----------
 * frame : Frame
 *   A frame to train the model on.
 * observation_columns : list of str
 *   Columns containing the observations.
 * column_scalings : list of double
 *   Column scalings for each of the observation columns.
 *   The scaling value is multiplied by the corresponding value in the
 *   observation column.
 * k : int (Optional)
 *   Desired number of clusters.
 *   This is an optional paramter with default value 2.
 * maxIterations : int (Optional)
 *   Number of iterations for which the algorithm should run.
 *   This is an optional paramter with default value 20.
 * epsilon : double (Optional)
 *   Distance threshold within which we consider k-means to have converged.
 *   This is an optional parameter with default value 1e-4.
 * initializationMode : str (Optional)
 *   The initialization technique for the algorithm.
 *   It could be either "random" or "k-means||".
 *   Default is "k-means||".
 */

@PluginDoc(oneLine = "Creates KMeans Model from train frame.",
  extended = "Upon training the 'k' cluster centers are computed.",
  returns = """dict
    Results.
    The data returned is composed of multiple components:
cluster_size : dict
    Cluster size
ClusterId : int
    Number of elements in the cluster 'ClusterId'.
within_set_sum_of_squared_error : double
    The set of sum of squared error for the model.""")
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
