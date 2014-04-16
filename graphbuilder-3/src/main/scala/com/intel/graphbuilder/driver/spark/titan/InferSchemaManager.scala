package com.intel.graphbuilder.driver.spark.titan

import com.intel.graphbuilder.elements.{Vertex, Edge}
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.parser.rule.DataTypeResolver
import com.intel.graphbuilder.schema.{InferSchemaFromData, GraphSchema, InferSchemaFromRules}
import com.intel.graphbuilder.write.titan.TitanSchemaWriter
import org.apache.spark.rdd.RDD

/**
 * Infers schema and writes as appropriate
 */
class InferSchemaManager(config: GraphBuilderConfig) {

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
