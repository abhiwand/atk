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

package com.intel.ia.giraph.lp

import com.intel.intelanalytics.domain.DomainJsonProtocol._
import com.intel.intelanalytics.domain.frame.FrameReference
import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.domain.model.ModelReference
import org.apache.commons.lang3.StringUtils

/**
 * Arguments to the plugin - see user docs for more on the parameters
 */
case class LabelPropagationArgs(model: ModelReference,
                                frame: FrameReference,
                                sourceIdColumnName: String,
                                destinationIdColumnName: String,
                                edgeWeightColumnName: String,
                                sourceIdLabelColumnName: String,
                                vectorValue: Boolean,
                                maxIterations: Option[Int] = None,
                                convergenceThreshold: Option[Double] = None,
                                anchorThreshold: Option[Double] = None,
                                lpLambda: Option[Double] = None,
                                validateGraphStructure: Option[Boolean] = None) {

  require(model != null, "model is required")
  require(frame != null, "frame is required")
  require(StringUtils.isNotBlank(sourceIdColumnName), "source column name property list is required")
  require(StringUtils.isNotBlank(destinationIdColumnName), "destination column name property list is required")
  require(StringUtils.isNotBlank(edgeWeightColumnName), "edge weight property list is required")
  require(StringUtils.isNotBlank(sourceIdLabelColumnName), "source label column name property list is required")
  require(maxIterations.isEmpty || maxIterations.get > 0, "Max iterations should be greater than 0")
}

case class LabelPropagationResult(value: String) //TODO

/** Json conversion for arguments and return value case classes */
object LabelPropagationJsonFormat {

  implicit val argsFormat = jsonFormat12(LabelPropagationArgs)
  implicit val resultFormat = jsonFormat1(LabelPropagationResult)
}