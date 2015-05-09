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

package com.intel.intelanalytics.domain.graph

import com.intel.intelanalytics.domain.graph.construction.FrameRule

/**
 * Command for loading  graph data into existing graph in the graph database. Source is tabular data from a dataframe
 * and it is converted into graph data using the graphbuilder3 graph construction rules.
 * @param graph Handle to the graph to be written to.
 * @param frameRules List of handles to the dataframe to be used as a data source.
 * @param append true to append to an existing graph, false otherwise.
 */
case class LoadGraphArgs(graph: GraphReference,
                         frameRules: List[FrameRule],
                         append: Boolean = false) {
  require(graph != null, "graph must not be null")
  require(frameRules != null, "frame rules must not be null")
}