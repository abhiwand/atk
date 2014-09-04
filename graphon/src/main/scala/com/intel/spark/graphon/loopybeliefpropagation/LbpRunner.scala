package com.intel.spark.graphon.loopybeliefpropagation

import org.apache.spark.rdd.RDD
import com.intel.graphbuilder.elements.{ Property, Vertex => GBVertex, Edge => GBEdge }

object LbpRunner {

  def runLbp(inVertices: RDD[GBVertex], inEdges: RDD[GBEdge], lbpParameters: Lbp): (RDD[GBVertex], RDD[GBEdge]) = {

    val outputPropertyLabel = lbpParameters.output_vertex_property_list.getOrElse("LBP_RESULT")

    val outVertices: RDD[GBVertex] = inVertices.map({
      case (GBVertex(physicalId, gbId, properties)) => GBVertex(physicalId, gbId, properties ++ Seq(Property(outputPropertyLabel, 0)))
    })

    (outVertices, inEdges)
  }
}
