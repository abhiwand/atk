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

package com.intel.intelanalytics.libSvmPlugins

import java.util.StringTokenizer

import com.intel.intelanalytics.UnitReturn
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.engine.plugin.{ ApiMaturityTag, ArgDoc, Invocation, PluginDoc }
import com.intel.intelanalytics.engine.spark.frame.SparkFrameData
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import libsvm.{ svm_node, svm_problem, svm_parameter, svm }
import org.apache.spark.frame.FrameRdd
import org.apache.spark.libsvm.ia.plugins.LibSvmJsonProtocol._

//Implicits needed for JSON conversion
import spray.json._

// TODO: all plugins should move out of engine-core into plugin modules

/*
Parameters
----------
frame : Frame
    A frame to train the model on.
label_column : str
    Column name containing the label for each observation.
observation_column : list of str
    Column(s) containing the observations.
epsilon: double (Optional)
    set tolerance of termination criterion.
    Default is 0.001.
degree: int (Optional)
    Degree of the polynomial kernel function ('poly').
    Ignored by all other kernels.
    Default is 3.
gamma: Double (Optional)
    Kernel coefficient for 'rbf', 'poly' and 'sigmoid'.
    Default is 1/n_features.
coef: double (Optional)
    Independent term in kernel function.
    It is only significant in 'poly' and 'sigmoid'.
    Default is 0.0.
nu : double (Optional)
    Set the parameter nu of nu-SVC, one-class SVM, and nu-SVR.
    Default is 0.5.
cache_size : double (Optional)
    Specify the size of the kernel cache (in MB).
    Default is 100.0.
shrinking : int (Optional)
    Whether to use the shrinking heuristic.
    Default is 1 (true).
probability : int (Optional)
    Whether to enable probability estimates.
    Default is 0 (false).
nr_weight : int (Optional)
    Default is 0.
c : double (Optional)
    Penalty parameter C of the error term.
    Default is 1.0.
p : double (Optional)
    Set the epsilon in loss function of epsilon-SVR.
    Default is 0.1.
svm_type: int (Optional)
    Set type of SVM.
    Default is 2.
    0 -- C-SVC
    1 -- nu-SVC
    2 -- one-class SVM
    3 -- epsilon-SVR
    4 -- nu-SVR
kernel_type: int (Optional)
    Specifies the kernel type to be used in the algorithm.
    Default is 2.
    0 -- linear: u\'\*v
    1 -- polynomial: (gamma*u\'\*v + coef0)^degree
    2 -- radial basis function: exp(-gamma*|u-v|^2)
    3 -- sigmoid: tanh(gamma*u\'\*v + coef0)
weight_label: Array[Int] (Optional)
    Default is (Array[Int](0))
weight: Array[Double] (Optional)
    Default is (Array[Double](0.0))
*/
@PluginDoc(oneLine = "Train Lib Svm model based on another frame.",
  extended = """Creating a lib Svm Model using the observation column and label column of the
train frame.""")
class LibSvmTrainPlugin extends SparkCommandPlugin[LibSvmTrainArgs, UnitReturn] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:libsvm/train"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: LibSvmTrainArgs)(implicit invocation: Invocation) = 1

  /**
   * Run LibSvm on the training frame and create a Model for it.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */

  override def execute(arguments: LibSvmTrainArgs)(implicit invocation: Invocation): UnitReturn = {
    val models = engine.models
    val modelRef = arguments.model
    val modelMeta = models.expectModel(modelRef)

    val frame: SparkFrameData = resolve(arguments.frame)
    // load frame as RDD
    val trainFrameRdd = frame.data

    //Running LibSVM
    val param = initializeParameters(arguments)
    val prob = initializeProblem(trainFrameRdd, arguments, param)

    svm.svm_check_parameter(prob, param) match {
      case null => None
      case str => Some(throw new IllegalArgumentException("Illegal Argument Exception: " + str))
    }
    val mySvmModel = svm.svm_train(prob, param)

    val jsonModel = new LibSvmData(mySvmModel, arguments.observationColumns)

    //TODO: Call save instead once implemented for models
    models.updateModel(modelMeta.toReference, jsonModel.toJson.asJsObject)
    new UnitReturn

  }

  /**
   * Initializes the libsvm params based on user's input else to default values
   *
   * @param arguments user supplied arguments for initializing the libsvm params
   * @return a data structure containing all the user supplied or default values for libsvm
   */
  private def initializeParameters(arguments: LibSvmTrainArgs): svm_parameter = {
    val param = new svm_parameter()

    // values for svm_parameters
    param.svm_type = arguments.getSvmType
    param.kernel_type = arguments.getKernelType
    param.degree = arguments.getDegree
    param.gamma = arguments.getGamma
    param.coef0 = arguments.getCoef0
    param.nu = arguments.getNu
    param.cache_size = arguments.getCacheSize
    param.C = arguments.getC
    param.eps = arguments.getEpsilon
    param.p = arguments.getP
    param.shrinking = arguments.getShrinking
    param.probability = arguments.getProbability
    param.nr_weight = arguments.getNrWeight
    param.weight_label = arguments.getWeightLabel
    param.weight = arguments.getWeight
    param
  }

  /**
   * Extracts the dataset from the provided Dataframe and converts into libsvm format
   *
   * @param trainFrameRdd Rdd containing the label and feature columns
   * @param arguments user supplied arguments for running this plugin
   * @param param data structure containing all the values for libsvm's exposed params
   * @return libsvm problem
   */

  private def initializeProblem(trainFrameRdd: FrameRdd, arguments: LibSvmTrainArgs, param: svm_parameter): svm_problem = {

    val observedRdd = trainFrameRdd.selectColumns(arguments.labelColumn +: arguments.observationColumns)
    val observedSchema = observedRdd.frameSchema.columns

    val output = LibSvmPluginFunctions.LibSvmFormatter(observedRdd)

    var vectory = Vector.empty[Double]
    var vectorx = Vector.empty[Array[svm_node]]
    var max_index: Int = 0
    val prob = new svm_problem()

    for (i <- 0 until output.length) {
      val observation = output(i)
      val splitObs: StringTokenizer = new StringTokenizer(observation, " \t\n\r\f:")

      vectory = vectory :+ LibSvmPluginFunctions.atof(splitObs.nextToken)
      val counter: Int = splitObs.countTokens / 2
      val x: Array[svm_node] = new Array[svm_node](counter)

      var j: Int = 0
      while (j < counter) {
        x(j) = new svm_node
        x(j).index = LibSvmPluginFunctions.atoi(splitObs.nextToken)
        x(j).value = LibSvmPluginFunctions.atof(splitObs.nextToken)
        j += 1
      }
      if (counter > 0) max_index = Math.max(max_index, x(counter - 1).index)
      vectorx = vectorx :+ x
    }

    prob.l = vectory.size
    prob.x = Array.ofDim[Array[svm_node]](prob.l)
    var k: Int = 0
    while (k < prob.l) {
      prob.x(k) = vectorx(k)
      k += 1
    }
    prob.y = Array.ofDim[Double](prob.l)
    var i: Int = 0
    while (i < prob.l) {
      prob.y(i) = vectory(i)
      i += 1
    }

    if (param.gamma == 0 && max_index > 0) param.gamma = 1.0 / max_index
    prob
  }

}
