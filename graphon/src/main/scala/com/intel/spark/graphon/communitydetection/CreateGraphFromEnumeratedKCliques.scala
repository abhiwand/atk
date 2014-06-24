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

package com.intel.spark.graphon.communitydetection

import com.intel.spark.graphon.communitydetection.KCliquePercolationDataTypes._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import com.intel.spark.graphon.communitydetection.KCliquePercolationDataTypes.ExtendersFact
import com.intel.spark.graphon.communitydetection.KCliquePercolationDataTypes.CliqueFact

object CreateGraphFromEnumeratedKCliques {

  /**
   * @param cliqueAndExtenders
   */
  def applyToExtendersFact(cliqueAndExtenders: RDD[ExtendersFact]) : RDD[Set[CliqueFact]] = {

    /**
     * Derive the key value pairs of k-1 cliques in the graph and k cliques that extend them
     * and drop the pointedness requirement
     */
    val cliqueAndExtendedClique: RDD[(CliqueFact, CliqueFact)] =
      cliqueAndExtenders.flatMap({ case ExtendersFact(clique, extenders, neighborHigh) => extenders.map(extendedBy => (CliqueFact(clique.members), CliqueFact(clique.members + extendedBy))) })

    /**
     * Group those pairs by their keys (the k-1) sets, so in each group we get something like
     * (U, Seq(V_1, …. V_m)), where the U is a k-1 clique and each V_i is a k-clique extending it
     */
    val cliqueAndExtendedCliqueSet: RDD[(CliqueFact, Set[CliqueFact])] = cliqueAndExtendedClique.groupBy(_._1).mapValues(_.map(_._2).toSet)

    /**
     * Each V_i becomes s vertex of the clique graph. Create edge list as ( V_i, V_j )
     */
    val cliqueEdgeList: RDD[Set[CliqueFact]] = cliqueAndExtendedCliqueSet.flatMap({ case (clique, setOfCliques) => setOfCliques.subsets(2) })

    cliqueEdgeList
  }

}
