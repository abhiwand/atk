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

package com.intel.intelanalytics.engine.spark.graph

//TODO: This should be replaced by a "storage" parameter on Graph and DataFrame that holds specifics like
// Titan table name and HDFS uris, etc. for those that need them.

/**
 * Utility for converting between user provided graph names and their names in the graph database.
 */
object GraphBackendName {

  private val iatGraphTablePrefix: String = "iat_graph_"

  /**
   * Converts the user's name for a graph into the name used by the underlying graph store.
   */
  def convertGraphUserNameToBackendName(graphName: String): String = {
    iatGraphTablePrefix + graphName
  }

  /**
   * Converts the name for a graph used by the underlying graph store to the name seen by users.
   */
  def convertGraphBackendNameToUserName(backendName: String): String = {
    backendName.stripPrefix(iatGraphTablePrefix)
  }
}
