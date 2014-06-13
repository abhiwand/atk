package com.intel.spark.graphon.communitydetection

import org.apache.spark.rdd.RDD
import com.intel.graphbuilder.elements.{Edge => GBEdge, GraphElement}
import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
import com.intel.spark.graphon.titanreader.TitanReader
import com.intel.spark.graphon.communitydetection.KCliquePercolationDataTypes.Edge


/**
 * Created by nlsegerl on 6/12/14.
 */
object KCliquePercolationDriver {

  def run(graphTableName: String, titanStorageHostName: String, sparkMaster: String, k: Int) = {


    val graphElements: RDD[GraphElement] =
      new TitanReader().loadGraph(graphTableName: String, titanStorageHostName: String, sparkMaster: String)

    val gbEdgeList = graphElements.filterEdges()
    val gb1 : RDD[Edge]= gbEdgeList.
      filter(e => (e.tailPhysicalId.asInstanceOf[Long] < e.headPhysicalId.asInstanceOf[Long])).
      map(e => KCliquePercolationDataTypes.Edge(e.tailPhysicalId.asInstanceOf[Long], e.headPhysicalId.asInstanceOf[Long]))

    val enumeratedKCliques = KCliqueEnumeration.applyToEdgeList(gb1, k)


  }



}
