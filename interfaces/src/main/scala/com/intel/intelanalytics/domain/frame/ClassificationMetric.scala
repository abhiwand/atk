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

package com.intel.intelanalytics.domain.frame

case class ClassificationMetric(frame: FrameReference, metricType: String, labelColumn: String, predColumn: String, posLabel: String, beta: Double) {
  require(frame != null, "ClassificationMetric requires a non-null dataframe.")
  require(metricType.equals("accuracy") ||
    metricType.equals("precision") ||
    metricType.equals("recall") ||
    metricType.equals("confusion_matrix") ||
    metricType.equals("f_measure"), "valid metric type is required")
  require(labelColumn != null && !labelColumn.equals(""), "label column is required")
  require(predColumn != null && !predColumn.equals(""), "predict column is required")
  require(posLabel != null && !posLabel.equals(""), "invalid positive label")
  require(beta > 0, "invalid beta value for f measure")
}

case class ClassificationMetricValue(metricValue: Option[Double] = None, valueList: Option[Seq[Long]] = None)
