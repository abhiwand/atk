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

package com.intel.graphbuilder.driver.local.examples

import com.intel.graphbuilder.elements.Edge
import com.intel.graphbuilder.elements.Vertex
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.parser._
import com.intel.graphbuilder.parser.rule.RuleParserDSL._
import com.intel.graphbuilder.parser.rule._
import com.intel.graphbuilder.schema.InferSchemaFromRules
import com.intel.graphbuilder.write.dao.{ EdgeDAO, VertexDAO }
import com.intel.graphbuilder.write.titan.TitanSchemaWriter
import com.intel.graphbuilder.write.{ EdgeWriter, VertexWriter }
import com.thinkaurelius.titan.core.TitanGraph
import java.util.Date
import org.apache.commons.configuration.BaseConfiguration
import scala.collection.JavaConversions._

// TODO: this class should either be deleted or cleaned up

/**
 * This driver does NOT use map/reduce or Spark.
 */
object LocalTitanCassandraDriver {

  def main(args: Array[String]) {

    val inputRows = List(
      List("1", "{(1)}", "1", "Y", "1", "Y"),
      List("2", "{(1)}", "10", "Y", "2", "Y"),
      List("3", "{(1)}", "11", "Y", "3", "Y"),
      List("4", "{(1),(2)}", "100", "N", "4", "Y"),
      List("5", "{(1)}", "101", "Y", "5", "Y")
    )

    val inputSchema = new InputSchema(List(
      new ColumnDef("cf:number", classOf[String]),
      new ColumnDef("cf:factor", classOf[String]),
      new ColumnDef("binary", classOf[String]),
      new ColumnDef("isPrime", classOf[String]),
      new ColumnDef("reverse", classOf[String]),
      new ColumnDef("isPalindrome", classOf[String])
    ))

    val vertexRules = List(VertexRule(gbId("cf:number"), List(property("isPrime"))), VertexRule(gbId("reverse")))
    val edgeRules = List(EdgeRule(gbId("cf:number"), gbId("reverse"), constant("reverseOf")))

    val dataTypeParser = new DataTypeResolver(inputSchema)
    val inferSchemaFromRules = new InferSchemaFromRules(dataTypeParser, vertexRules, edgeRules)

    println("--- Inferred Schema ---")
    println("canInferEdgeLabels: " + inferSchemaFromRules.canInferEdgeLabels)
    println("canInferAllPropertyKeyNames: " + inferSchemaFromRules.canInferAllPropertyKeyNames)

    val graphSchema = inferSchemaFromRules.inferGraphSchema()

    println("--- Inferred Schema ---")
    println("edgeLabelDefs: " + graphSchema.edgeLabelDefs)
    println("propertyDefs: " + graphSchema.propertyDefs)

    val parser = new CombinedParser(inputSchema, new VertexRuleParser(inputSchema, vertexRules), new EdgeRuleParser(inputSchema, edgeRules))

    val elements = inputRows.flatMap(row => parser.parse(row))

    // Separate Vertices and Edges
    val vertices = elements.collect {
      case v: Vertex => v
    }
    val edges = elements.collect {
      case e: Edge => e
    }

    // Print out the parsed Info
    println("--- Parsing Results ---")
    println("elements size: " + elements.size)
    println("vertices size: " + vertices.size)
    println("edges size: " + edges.size)
    elements.foreach(element => println(element))

    // Merge Duplicates (non-Spark)
    val mergedVertices = vertices.groupBy(v => v.id).mapValues(dups => dups.reduce((v1, v2) => v1.merge(v2))).values.toList
    val mergedEdges = edges.groupBy(e => e.id).mapValues(dups => dups.reduce((e1, e2) => e1.merge(e2))).values.toList

    println("\n--- Merge Duplicates ---")
    println("mergedVertices size: " + mergedVertices.size)
    println("mergedEdges size: " + mergedEdges.size)

    // Connect to Titan
    val titanConfig = new BaseConfiguration()
    titanConfig.setProperty("storage.backend", "cassandra")
    titanConfig.setProperty("storage.hostname", "127.0.0.1")
    titanConfig.setProperty("storage.keyspace", "titan")
    val titanConnector = new TitanGraphConnector(titanConfig)
    var graph = titanConnector.connect()

    try {

      // write the Graph Schema
      val titanSchemaWriter = new TitanSchemaWriter(graph.asInstanceOf[TitanGraph])
      titanSchemaWriter.write(graphSchema)

      // setup writers
      val vertexDAO = new VertexDAO(graph)
      val vertexWriter = new VertexWriter(vertexDAO, append = false)
      val edgeWriter = new EdgeWriter(new EdgeDAO(graph, vertexDAO), append = false)

      // write Graph
      mergedVertices.foreach(v => {
        val bp = vertexWriter.write(v)
        println("ID => " + bp.getId + " --- " + bp + " --- " + v)
      })
      mergedEdges.foreach(e => {
        val bp = edgeWriter.write(e)
        println("Edge Id => " + bp.getId + " --- " + bp + " ---- " + e)
      })

      // Results
      println(graph.getEdges.iterator().toList.size)
      println(graph.getVertices.iterator().toList.size)

    }
    finally {
      graph.shutdown()
    }

    println("done " + new Date())
  }

}

