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

import com.intel.intelanalytics.domain.frame.FrameReference
import com.intel.intelanalytics.domain.model.ModelReference
import com.intel.intelanalytics.engine.plugin.{ ArgDoc, Invocation }

/**
 * Command for training a lib svm model with the provided dataset and params.
 * @param model Handle to the model to be used.
 * @param frame Handle to the data frame
 * @param observationColumns Handle to the observation column/s of the data frame
 */
case class LibSvmTrainArgs(@ArgDoc("""Handle to the model to be used.""") model: ModelReference,

                           @ArgDoc("""A frame to train the model on.""") frame: FrameReference,

                           @ArgDoc("""Column name containing the label for each observation.""") labelColumn: String,

                           @ArgDoc("""Column(s) containing the observations.""") observationColumns: List[String],

                           @ArgDoc("""Set type of SVM.
Default is 2.

                            0 -- C-SVC

                            1 -- nu-SVC

                            2 -- one-class SVM

                            3 -- epsilon-SVR

                            4 -- nu-SVR""") svmType: Option[Int],

                           @ArgDoc("""Specifies the kernel type to be used in the algorithm.
Default is 2.

                            0 -- linear: u\'\*v

                            1 -- polynomial: (gamma*u\'\*v + coef0)^degree

                            2 -- radial basis function: exp(-gamma*|u-v|^2)

                            3 -- sigmoid: tanh(gamma*u\'\*v + coef0)""") kernelType: Option[Int],

                           @ArgDoc("""Default is (Array[Int](0))""") weightLabel: Option[Array[Int]],

                           @ArgDoc("""Default is (Array[Double](0.0))""") weight: Option[Array[Double]],

                           @ArgDoc("""Set tolerance of termination criterion.
Default is 0.001.""") epsilon: Option[Double] = None,

                           @ArgDoc("""Degree of the polynomial kernel function ('poly').
Ignored by all other kernels.
Default is 3.""") degree: Option[Int] = None,

                           @ArgDoc("""Kernel coefficient for 'rbf', 'poly' and 'sigmoid'.
Default is 1/n_features.""") gamma: Option[Double] = None,

                           @ArgDoc("""Independent term in kernel function.
It is only significant in 'poly' and 'sigmoid'.
Default is 0.0.""") coef: Option[Double] = None,

                           @ArgDoc("""Set the parameter nu of nu-SVC, one-class SVM, and nu-SVR.
Default is 0.5.""") nu: Option[Double] = None,

                           @ArgDoc("""Specify the size of the kernel cache (in MB).
Default is 100.0.""") cacheSize: Option[Double] = None,

                           @ArgDoc("""Whether to use the shrinking heuristic.
Default is 1 (true).""") shrinking: Option[Int] = None,

                           @ArgDoc("""Whether to enable probability estimates.
Default is 0 (false).""") probability: Option[Int] = None,

                           @ArgDoc("""Default is 0.""") nrWeight: Option[Int] = None,

                           @ArgDoc("""Penalty parameter C of the error term.
Default is 1.0.""") C: Option[Double] = None,

                           @ArgDoc("""Set the epsilon in loss function of epsilon-SVR.
Default is 0.1.""") p: Option[Double] = None) {
  require(model != null, "model must not be null")
  require(frame != null, "frame must not be null")
  require(observationColumns != null && !observationColumns.isEmpty, "One or more observation columns is required")

  def getEpsilon: Double = {
    if (epsilon.isDefined) {
      require(epsilon.get > 0.0, "epsilon must be a positive value")
    }
    epsilon.getOrElse(0.001)
  }

  def getDegree: Int = {
    if (degree.isDefined) {
      require(degree.get > 0, "degree must be a positive value")
    }
    degree.getOrElse(3)
  }

  def getGamma: Double = {
    gamma.getOrElse(1 / observationColumns.length)
  }

  def getNu: Double = {
    nu.getOrElse(0.5)
  }

  def getCoef0: Double = {
    coef.getOrElse(0.0)
  }

  def getCacheSize: Double = {
    cacheSize.getOrElse(100.0)
  }

  def getP: Double = {
    p.getOrElse(0.1)
  }

  def getShrinking: Int = {
    shrinking.getOrElse(1)
  }

  def getProbability: Int = {
    probability.getOrElse(0)
  }

  def getNrWeight: Int = {
    nrWeight.getOrElse(1)
  }

  def getC: Double = {
    C.getOrElse(1.0)
  }

  def getSvmType: Int = {
    svmType.getOrElse(2)
  }

  def getKernelType: Int = {
    kernelType.getOrElse(2)
  }

  def getWeightLabel: Array[Int] = {
    weightLabel.getOrElse(Array[Int](0))
  }

  def getWeight: Array[Double] = {
    weight.getOrElse(Array[Double](0.0))
  }
}
