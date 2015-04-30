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

package com.intel.graphbuilder.driver.spark.titan

import com.intel.graphbuilder.parser.InputSchema
import com.intel.graphbuilder.parser.rule.{ EdgeRule, VertexRule }
import com.intel.graphbuilder.util.SerializableBaseConfiguration

/**
 * Configuration options for GraphBuilder
 *
 * @param inputSchema describes the columns of input
 * @param vertexRules rules for parsing Vertices
 * @param edgeRules rules for parsing Edges
 * @param titanConfig connect to Titan
 * @param append true to append to an existing Graph, slower because each write requires a read (incremental graph construction).
 * @param retainDanglingEdges true to add extra vertices for dangling edges, false to drop dangling edges
 * @param inferSchema true to automatically infer the schema from the rules and, if needed, the data, false if the schema is already defined.
 * @param broadcastVertexIds experimental feature where gbId to physicalId mappings will be broadcast by spark instead
 *                           of doing the usual an RDD JOIN.  Should only be true if all of the vertices can fit in
 *                           memory of both the driver and each executor.
 *                           Theoretically, this should be faster but shouldn't scale as large as when it is false.
 *                           This feature does not perform well yet for larger data sizes (it is hitting some other
 *                           bottleneck while writing edges). 23GB Netflix data produced about 180MB of Vertex Ids.
 */
case class GraphBuilderConfig(inputSchema: InputSchema,
                              vertexRules: List[VertexRule],
                              edgeRules: List[EdgeRule],
                              titanConfig: SerializableBaseConfiguration,
                              append: Boolean = false,
                              retainDanglingEdges: Boolean = false,
                              inferSchema: Boolean = true,
                              broadcastVertexIds: Boolean = false) extends Serializable {

}
