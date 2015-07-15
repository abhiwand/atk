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
package org.apache.spark.mllib.ia.plugins.dimensionalityreduction

import com.intel.taproot.analytics.domain.frame.{ FrameEntity, FrameReference }
import com.intel.taproot.analytics.domain.model.ModelReference

/**
 * Input arguments for principal components predict plugin
 * @param model Principal component count
 * @param frame Handle to the observation columns of the data frame
 * @param observationColumns Handle to the observation columns of the data frame. Defaults to the columns used to train the model
 * @param c Principal components count for predict operation. Defaults to count value used to train the model
 * @param tSquareIndex Indicator for whether the t squared index is to be computed. Defaults to false
 * @param name Name of the output frame
 */

case class PrincipalComponentsPredictArgs(model: ModelReference,
                                          frame: FrameReference,
                                          observationColumns: Option[List[String]] = None,
                                          c: Option[Int] = None,
                                          tSquareIndex: Option[Boolean] = None,
                                          name: Option[String] = None) {
  require(model != null, "model is required")
  require(frame != null, "frame is required")
}

/**
 * Return of principal components predict plugin
 * @param outputFrame A new frame with existing columns and columns containing the projections on it
 * @param tSquaredIndex t-square index value if requested
 */
case class PrincipalComponentsPredictReturn(outputFrame: FrameEntity, tSquaredIndex: Option[Double])