package com.intel.graphbuilder.driver.spark.titan

import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.parser.rule._
import java.text.NumberFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import com.intel.graphbuilder.parser.Parser
import com.intel.graphbuilder.elements.Vertex

/**
 * This is a GraphBuilder that runs on Spark, uses a RuleParser and creates Graphs in Titan.
 *
 * @param config configuration options
 */
class GraphBuilder(config: GraphBuilderConfig) extends Serializable {

  val titanConnector = new TitanGraphConnector(config.titanConfig)
  val titanSchemaManager = new InferSchemaManager(config)
  val vertexParser = new VertexRuleParser(config.inputSchema, config.vertexRules)
  val edgeParser = new EdgeRuleParser(config.inputSchema, config.edgeRules)

  /**
   * Build the Graph
   *
   * @param inputRdd the input rows to create the graph from
   */
  def build(inputRdd: RDD[Seq[_]]) {
    if (config.inferSchema) {
      titanSchemaManager.writeSchemaFromRules()
    }
    buildGraphWithSpark(inputRdd)
  }

  /**
   * Build the Graph using Spark
   *
   * @param inputRdd the input rows to create the graph from
   */
  def buildGraphWithSpark(inputRdd: RDD[Seq[_]]) {

    println("Parse and Write Vertices")
    var vertices = inputRdd.parseVertices(vertexParser)
    var edges = inputRdd.parseEdges(edgeParser)

    if (config.retainDanglingEdges) {
      println("retain dangling edges means we parse edges now too")
      vertices = vertices.union(edges.verticesFromEdges())
    }

    if (config.inferSchema && titanSchemaManager.needsToInferSchemaFromData) {
      println("inferring schema from data")
      titanSchemaManager.writeSchemaFromData(edges, vertices)
    }

    val mergedVertices = vertices.mergeDuplicates()
    val idMap = mergedVertices.write(titanConnector, config.append)
    idMap.persist(StorageLevel.MEMORY_AND_DISK)
    println("done parsing and writing, vertices count: " + NumberFormat.getInstance().format(idMap.count()))

    println("parsing and merging edges")
    if (config.biDirectional) {
      edges = edges.biDirectional()
    }
    val mergedEdges = edges.mergeDuplicates()
    //println("done parsing and merging, edge count: " + NumberFormat.getInstance().format(mergedEdges.count()))

    println("join with physical ids")
    val edgesWithPhysicalIds = mergedEdges.joinWithPhysicalIds(idMap)

    println("starting write of edges")
    edgesWithPhysicalIds.write(titanConnector, config.append)
    println("done writing edges")
  }

}
