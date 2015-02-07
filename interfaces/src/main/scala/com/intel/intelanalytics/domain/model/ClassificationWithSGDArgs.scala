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

package com.intel.intelanalytics.domain.model

import com.intel.intelanalytics.domain.frame.FrameReference

/**
 * Command for loading model data into existing model in the model database.
 * @param model Handle to the model to be written to.
 * @param frame Handle to the data frame
 * @param observationColumns Handle to the observation column of the data frame
 * @param labelColumn Handle to the label column of the data frame
 */
case class ClassificationWithSGDArgs(model: ModelReference,
                                     frame: FrameReference,
                                     observationColumns: List[String],
                                     labelColumn: String,
                                     intercept:Option[Boolean]) {
  require(model != null, "model must not be null")
  require(frame != null, "frame must not be null")
  require(!observationColumns.isEmpty && observationColumns != null, "observationColumn must not be null nor empty")
  require(!labelColumn.isEmpty && labelColumn != null, "labelColumn must not be null nor empty")

  def getIntercept : Boolean = {
    intercept.getOrElse(false)
  }
}
