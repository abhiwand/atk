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

package com.intel.intelanalytics.domain.graph.construction

/**
 * IntelAnalytics V1 API Graph Loader rule for creating edges from tabular data.
 * @param head Rule for specifying head (destination) vertex of edge.
 * @param tail Rule for specifying tail (source) vertex of edge.
 * @param label Label of the edge.
 * @param properties List of rules for generating properties of the edge.
 * @param bidirectional True if the edge is a bidirectional edge, false if it is a directed edge.
 */
case class EdgeRule(head: PropertyRule,
                    tail: PropertyRule,
                    label: ValueRule,
                    properties: List[PropertyRule],
                    bidirectional: Boolean) {
  require(head != null, "head must not be null")
  require(tail != null, "tail must not be null")
  require(label != null, "lavel must not be null")
}