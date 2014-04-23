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

package com.intel.graphbuilder.driver.spark.titan.examples

import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import java.util.Date

/**
 * Utility for use during development.
 */
object DumpTitanGraph {

  // Titan Settings
  val titanConfig = new SerializableBaseConfiguration()
  titanConfig.setProperty("storage.backend", "cassandra")
  titanConfig.setProperty("storage.hostname", "127.0.0.1")
  titanConfig.setProperty("storage.keyspace", "netflix")

  def main(args: Array[String]) = {

    val titanConnector = new TitanGraphConnector(titanConfig)

    val graph = titanConnector.connect()
    try {
      println(ExamplesUtils.dumpGraph(graph))
    }
    finally {
      graph.shutdown()
    }

    println("done " + new Date())

  }
}
