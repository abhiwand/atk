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

/**
 * Command for training a lib svm model with the provided dataset and params.
 * @param model Handle to the model to be used.
 * @param frame Handle to the data frame
 * @param observationColumns Handle to the observation column/s of the data frame
 */
case class LibSvmTrainArgs(model: ModelReference,
                           frame: FrameReference,
                           labelColumn: String,
                           observationColumns: List[String],
                           svmType: Option[Int],
                           kernelType: Option[Int],
                           weightLabel: Option[Array[Int]],
                           weight: Option[Array[Double]],
                           epsilon: Option[Double] = None,
                           degree: Option[Int] = None,
                           gamma: Option[Double] = None,
                           coef: Option[Double] = None,
                           nu: Option[Double] = None,
                           cacheSize: Option[Double] = None,
                           shrinking: Option[Int] = None,
                           probability: Option[Int] = None,
                           nrWeight: Option[Int] = None,
                           C: Option[Double] = None,
                           p: Option[Double] = None) {
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
