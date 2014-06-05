package com.intel.spark.graphon.communitydetection


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

  def runKCliqueCommunityAnalysis(input : KCliqueInput) {

  }

}
