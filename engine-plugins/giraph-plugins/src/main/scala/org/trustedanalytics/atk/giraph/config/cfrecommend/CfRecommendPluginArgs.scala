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

package org.trustedanalytics.atk.giraph.config.cfrecommend

import org.trustedanalytics.atk.domain.frame.{ FrameEntity, FrameReference }
import org.trustedanalytics.atk.domain.model.ModelReference
import org.apache.commons.lang3.StringUtils

/**
 * Arguments to the CfRecommend plugin - see user docs for more on the parameters
 */
case class CfRecommendArgs(model: ModelReference,
                           userFrame: FrameReference,
                           itemFrame: FrameReference,
                           userColumnName: String,
                           itemColumnName: String,
                           cfFactorsColumnName: String,
                           numFactors: Int) {

  require(model != null, "model is required")
  require(userFrame != null, "frame is required")
  require(itemFrame != null, "frame is required")
  require(StringUtils.isNotBlank(userColumnName), "user column name is required")
  require(StringUtils.isNotBlank(itemColumnName), "item column name is required")
  require(StringUtils.isNotBlank(cfFactorsColumnName), "factors column name is required")
  require(numFactors > 0, "factors column size must be greater than 0")

}

case class CfRecommendResult(recommendResults: FrameEntity) {
  require(recommendResults != null, "document results are required")
}

/** Json conversion for arguments and return value case classes */
object CfRecommendJsonFormat {
  import org.trustedanalytics.atk.domain.DomainJsonProtocol._

  implicit val cfRecommendFormat = jsonFormat7(CfRecommendArgs)
  implicit val cfRecommendResultFormat = jsonFormat1(CfRecommendResult)
}
