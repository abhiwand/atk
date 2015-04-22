package com.intel.spark.graphon.hierarchicalclustering

import java.io.Serializable

import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.schema.{ EdgeLabelDef, GraphSchema, PropertyDef, PropertyType }
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.graphbuilder.write.titan.TitanSchemaWriter
import com.intel.intelanalytics.domain.schema.GraphSchema
import com.thinkaurelius.titan.core.TitanGraph
import com.tinkerpop.blueprints.{ Edge, Vertex }

trait HierarchicalClusteringStorageInterface extends Serializable {

  def addSchema(): Unit

  def addVertexAndEdges(src: Long, dest: Long, metaNodeCount: Long, metaNodeName: String, iteration: Int): Long

  def commit(): Unit

  def shutdown(): Unit
}
