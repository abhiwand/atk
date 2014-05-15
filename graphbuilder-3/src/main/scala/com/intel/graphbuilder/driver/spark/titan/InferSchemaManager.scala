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

package com.intel.graphbuilder.driver.spark.titan

import com.intel.graphbuilder.elements.{ Vertex, Edge }
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.parser.rule.DataTypeResolver
import com.intel.graphbuilder.schema.{ InferSchemaFromData, GraphSchema, InferSchemaFromRules }
import com.intel.graphbuilder.write.titan.TitanSchemaWriter
import org.apache.spark.rdd.RDD

/**
 * Infers schema and writes as appropriate
 */
class InferSchemaManager(config: GraphBuilderConfig) extends Serializable {

  val titanConnector = new TitanGraphConnector(config.titanConfig)
  val dataTypeResolver = new DataTypeResolver(config.inputSchema)
  val inferSchemaFromRules = new InferSchemaFromRules(dataTypeResolver, config.vertexRules, config.edgeRules)
  val inferSchemaFromData = new InferSchemaFromData()

  /**
   * True if the entire schema was not infer-able from the rules
   */
  def needsToInferSchemaFromData: Boolean = {
    !inferSchemaFromRules.canInferAll
  }

  /**
   * Write the Inferred Schema to Titan, as much as possible.
   */
  def writeSchemaFromRules() = {
    writeSchema(inferSchemaFromRules.inferGraphSchema())
  }

  /**
   * Infer the schema by passing over each edge and vertex.
   */
  def writeSchemaFromData(edges: RDD[Edge], vertices: RDD[Vertex]) = {
    edges.foreach(edge => inferSchemaFromData.add(edge))
    vertices.foreach(vertex => inferSchemaFromData.add(vertex))
    writeSchema(inferSchemaFromData.graphSchema)
  }

  /**
   * Write the supplied schema to Titan
   */
  private def writeSchema(graphSchema: GraphSchema) = {
    val graph = titanConnector.connect()
    try {
      println("Writing the schema to Titan")
      val writer = new TitanSchemaWriter(graph)
      writer.write(graphSchema)
      graph.commit()
    }
    finally {
      graph.shutdown()
    }
  }
}
