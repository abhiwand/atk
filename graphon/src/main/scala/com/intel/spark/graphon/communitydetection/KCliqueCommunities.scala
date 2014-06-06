package com.intel.spark.graphon.communitydetection

import com.intel.spark.graphon.titanreader.TitanReader
import com.intel.graphbuilder.elements.{GraphElement, Edge => GBEdge}
import org.apache.spark.rdd.RDD
import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
import com.intel.graphbuilder.driver.spark.rdd.GraphElementRDDFunctions
/**
 * The command for running a k-clique community analysis
 * @param k  Parameter determining clique-size used to determine communities. Must be at least 1. Large values of k
 *           result in fewer, smaller communities that are more connected
 * @param dataFrame This should be reference to the graph from which we are pulling data
 * @param modelOutputPath This should a reference to the graph to which we are writing the data
 *
 */
case class KCliqueInput(k: Int,
                        dataFrame: String,
                        modelOutputPath: String)

class KCliqueCommunities {

  // TODO: pipe these in from the input
  val graphName = "kclique_testgraph"
  val titanStorageName = "localhost"
  val sparkMasterName = "localhost"

  def runKCliqueCommunityAnalysis(input : KCliqueInput) {

    val titanReader = new TitanReader()
    val graphElements : RDD[GraphElement] =  titanReader.loadGraph(graphName, titanStorageName, sparkMasterName)

    val gbEdgeList : RDD[GBEdge] = graphElements.filterEdges()


    // GB allows these to be pairs of anys, but Titan actually gives us longs.
    // it would be a total soup if we let the vertices have type "any" so convert to longs

    val edgeList  = gbEdgeList.map(e => Pair(e.headPhysicalId, e.tailPhysicalId))
      .filter( _.isInstanceOf[(Long, Long)]).asInstanceOf[(Long, Long)]




  }

}
