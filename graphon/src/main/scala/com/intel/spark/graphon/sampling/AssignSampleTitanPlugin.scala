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

package com.intel.spark.graphon.sampling

import com.intel.graphbuilder.elements.{ GBEdge, Property, GBVertex }
import com.intel.intelanalytics.UnitReturn
import com.intel.intelanalytics.domain.graph.{ AssignSampleTitanArgs, GraphEntity }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.context.SparkContextFactory
import com.intel.intelanalytics.engine.spark.frame.plugins.assignsample.MLDataSplitter
import com.intel.intelanalytics.engine.spark.graph.SparkGraphHBaseBackend
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.rdd.RDD

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Assign classes to rows.
 */
class AssignSampleTitanPlugin extends SparkCommandPlugin[AssignSampleTitanArgs, UnitReturn] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "graph:titan/sampling/assign_sample"

  //TODO remove when we move to the next version of spark
  override def kryoRegistrator: Option[String] = None

  override def numberOfJobs(arguments: AssignSampleTitanArgs)(implicit invocation: Invocation): Int = 4

  /**
   * Assign classes to rows.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: AssignSampleTitanArgs)(implicit invocation: Invocation): UnitReturn = {
    //Titan Settings
    val config = configuration

    val graph = engine.graphs.expectGraph(arguments.graph.id)
    require(graph.isTitan, "assign sample is currently only implemented for Titan Graphs")
    sc.addJar(SparkContextFactory.jarPath("graphon"))

    //convert graph into vertex and edge RDDs
    val gbVertices = engine.graphs.loadGbVertices(sc, graph)

    val splitRDD: RDD[GBVertex] = splitVertexRDD(gbVertices,
      arguments.samplePercentages,
      arguments.getSampleLabels,
      arguments.getOutputProperty,
      arguments.getRandomSeed)
    //GB Edges are unchanged so we do not need to supply the edges
    val emptyRDD: RDD[GBEdge] = sc.parallelize(Nil)
    engine.graphs.writeToTitan(SparkGraphHBaseBackend.getHBaseTableNameFromGraphEntity(graph), splitRDD, emptyRDD, append = true)
    new UnitReturn
  }

  /**
   * Split Vertex RDD into samples
   * @param gbVertices RDD representation of Titan Vertices
   * @param samplePercentages approximate percentages for each group
   * @param sampleLabels label to assign for each group
   * @param outputProperty property key to use for label
   * @param seed random seed value
   * @return SplitLabeled RDD
   */
  def splitVertexRDD(gbVertices: RDD[GBVertex], samplePercentages: List[Double], sampleLabels: List[String],
                     outputProperty: String, seed: Int): RDD[GBVertex] = {
    // run the operation
    val splitter = new MLDataSplitter(samplePercentages.toArray, sampleLabels.toArray, seed)
    val labeledRDD = splitter.randomlyLabelRDD(gbVertices)
    //we are appending a single property, we do not need to specify more than that.
    val splitRDD = labeledRDD.map(labeledRow => GBVertex(
      labeledRow.entry.physicalId,
      labeledRow.entry.gbId,
      Set(Property(outputProperty, labeledRow.label.asInstanceOf[Any]))))
    splitRDD
  }

}
