package com.intel.spark.graphon.hierarchicalclustering

import com.intel.graphbuilder.elements.{ GBEdge, GBVertex }
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import org.apache.spark.rdd.RDD
import java.io.{ Serializable }

object HierarchicalClusteringImpl extends Serializable {

  def execute(vertices: RDD[GBVertex], edges: RDD[GBEdge], titanConfig: SerializableBaseConfiguration): Unit = {

    val graphAdRdd: RDD[HierarchicalClusteringEdge] = edges.map {
      case e => HierarchicalClusteringEdge(e.headPhysicalId.asInstanceOf[Number].longValue,
        HierarchicalClusteringConstants.DefaultNodeCount,
        e.tailPhysicalId.asInstanceOf[Number].longValue,
        HierarchicalClusteringConstants.DefaultNodeCount,
        1 - e.getProperty("dist").get.value.asInstanceOf[Float], false)
    }.distinct()

    HierarchicalClusteringMain.execute(graphAdRdd, titanConfig)
  }
}
