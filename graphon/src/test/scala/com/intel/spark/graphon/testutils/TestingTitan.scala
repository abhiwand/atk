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

package com.intel.spark.graphon.testutils

import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.testutils.{ LogUtils, DirectoryUtils }
import com.thinkaurelius.titan.core.util.TitanCleanup
import com.thinkaurelius.titan.core.{ TitanFactory, TitanGraph }
import com.tinkerpop.blueprints.Graph
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph
import org.scalatest.{ BeforeAndAfter, FlatSpec }
import java.io.File

import scala.concurrent.Lock

/**
 * This trait can be mixed into Specifications to get a TitanGraph backed by an in-memory database
 * for testing purposes.
 *
 * The TitanGraph is wrapped by IdGraph to allow tests to create vertices and edges with specific Ids.
 * @see com.tinkerpop.blueprints.util.wrappers.id.IdGraph
 *
 * IMPORTANT! only one thread can use the graph below at a time.
 */
trait TestingTitan extends FlatSpec with BeforeAndAfter {

  LogUtils.silenceTitan()

  var tmpDir: File = DirectoryUtils.createTempDirectory("titan-graph-for-unit-testing-")

  val titanConfig = new SerializableBaseConfiguration()
  titanConfig.setProperty("storage.directory", tmpDir.getAbsolutePath)

  var titanConnector = new TitanGraphConnector(titanConfig)
  var graph = titanConnector.connect()

  after (
    cleanupTitan()
  )

  /**
   * IMPORTANT! Closes in-memory graph used for testing
   */
  def cleanupTitan(): Unit = {
    try {
      if (graph != null) {
        graph.shutdown()
      }
    }
    finally {
      graph = null
      TestingTitan.lock.release()
    }

  }
}

object TestingTitan {
  private val lock = new Lock()
}
