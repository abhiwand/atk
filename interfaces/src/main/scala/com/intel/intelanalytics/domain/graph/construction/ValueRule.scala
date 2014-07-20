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

package com.intel.intelanalytics.domain.graph.construction

/**
 * IntelAnalytics V1 API Graph Loader rule for creating property graph values from tabular data.
 * @param source Is the value constant or varying?
 * @param value If a constant, the value taken. If a varying, the name of the column from which the data is parsed.
 */
case class ValueRule(source: String, value: String) {
  require(source.equals(GBValueSourcing.CONSTANT) || source.equals(GBValueSourcing.VARYING),
    s"source must be one of (${GBValueSourcing.CONSTANT}, ${GBValueSourcing.VARYING})")
  require(value != null, "value must not be null")
}

