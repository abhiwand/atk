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

package com.intel.spark.graphon.loopybeliefpropagation

import org.apache.spark.rdd.RDD
import com.intel.graphbuilder.elements.{ Edge => GBEdge, Vertex => GBVertex, Property }
import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
import org.apache.spark.SparkContext
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.driver.spark.titan.reader.TitanReader
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.spark.graphon.communitydetection.kclique.datatypes.Edge
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.driver.spark.titan.reader.TitanReader
import com.intel.spark.graphon.communitydetection.kclique.CliqueEnumerator
import com.intel.spark.graphon.communitydetection.kclique.GraphGenerator
import com.intel.spark.graphon.communitydetection.kclique.GetConnectedComponents
import com.intel.spark.graphon.communitydetection.kclique.CommunityAssigner
import com.intel.spark.graphon.communitydetection.kclique.GBVertexRDDBuilder
import com.intel.spark.graphon.communitydetection.kclique.CommunityWriterInTitan
import com.intel.spark.graphon.communitydetection.kclique.datatypes.Edge
import com.intel.spark.graphon.communitydetection.kclique.datatypes
import com.intel.spark.graphon.communitydetection.ScalaToJavaCollectionConverter
import com.intel.graphbuilder.driver.spark.titan.{ GraphBuilderConfig, GraphBuilder }
import com.intel.graphbuilder.parser.InputSchema

/**
 * The driver for running loopy belief propagation
 */
object Driver {

  /**
   * The main driver to execute k-clique percolation algorithm
   * @param titanConfig The titan configuration for input
   * @param sc SparkContext
   * @param outputPropertyLabel the label to which the output is to be written
   */
  def run(titanConfig: SerializableBaseConfiguration, sc: SparkContext, outputPropertyLabel: String) = {

    // Create the Titan connection
    val titanConnector = new TitanGraphConnector(titanConfig)

    // Read the graph from Titan
    val titanReader = new TitanReader(sc, titanConnector)
    val titanReaderRDD = titanReader.read()

    // Get the GraphBuilder vertex list
    val gbVertices = titanReaderRDD.filterVertices()

    // Get the GraphBuilder edge list
    val gbEdges = titanReaderRDD.filterEdges()

    // do a little GraphX MagiX

    val newGBVertices: RDD[GBVertex] = gbVertices.map({
      case (GBVertex(physicalId, gbId, properties)) => GBVertex(physicalId, gbId, Seq(Property(outputPropertyLabel, 0)))
    })

    // Create the GraphBuilder object
    // Setting true to append for updating existing graph
    val gb = new GraphBuilder(new GraphBuilderConfig(new InputSchema(Seq.empty), List.empty, List.empty, titanConfig, append = true))
    // Build the graph using spark
    gb.buildGraphWithSpark(newGBVertices, gbEdges)

  }

}
