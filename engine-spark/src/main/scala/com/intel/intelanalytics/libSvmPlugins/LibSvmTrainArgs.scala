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

package com.intel.intelanalytics.libSvmPlugins

import com.intel.intelanalytics.domain.frame.FrameReference
import com.intel.intelanalytics.domain.model.ModelReference

/**
 * Command for loading model data into existing model in the model database.
 * @param model Handle to the model to be written to.
 * @param frame Handle to the data frame
 * @param observationColumns Handle to the observation column of the data frame
 * @param epsilon Optional distance threshold within which we've considered centers to have converged. Default is 1e-4
 */
case class LibSvmTrainArgs(model: ModelReference,
                           frame: FrameReference,
                           labelColumn: String,
                           observationColumns: List[String],
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
                           p: Option[Double] = None,
                           svmType: Option[Int],
                           kernelType: Option[Int],
                           weightLabel: Option[Array[Int]],
                           weight: Option[Array[Double]]) {
  require(model != null, "model must not be null")
  require(frame != null, "frame must not be null")
  require(observationColumns != null && !observationColumns.isEmpty, "observationColumn must not be null nor empty")

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
    gamma.getOrElse(1 / 9)
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

