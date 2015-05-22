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

package com.intel.testutils

import java.io.File

import com.thinkaurelius.titan.core.{ TitanFactory, TitanGraph }
import com.tinkerpop.blueprints.{ Vertex, Graph }
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph
import org.apache.commons.configuration.BaseConfiguration

/**
 * This trait can be mixed into Tests to get a TitanGraph backed by Berkeley for testing purposes.
 *
 * The TitanGraph can be wrapped by IdGraph to allow tests to create vertices and edges with specific Ids.
 *
 * IMPORTANT! only one thread can use the graph below at a time. This isn't normally an issue because
 * each test usually gets its own copy.
 *
 * IMPORTANT! You must call cleanupTitan() when you are done
 */

trait TestingTitan {
  LogUtils.silenceTitan()

  var tmpDir: File = null
  var titanBaseConfig: BaseConfiguration = null
  var titanGraph: TitanGraph = null
  var titanIdGraph: Graph = null //ID graph is used for setting user-specified vertex Ids

  def setupTitan(): Unit = {
    tmpDir = DirectoryUtils.createTempDirectory("titan-graph-for-unit-testing-")

    titanBaseConfig = new BaseConfiguration()
    titanBaseConfig.setProperty("storage.backend", "berkeleyje")
    titanBaseConfig.setProperty("storage.batch-loading", "true")
    titanBaseConfig.setProperty("storage.directory", tmpDir.getAbsolutePath)

    //Trying to fix OutOfMemory errors during builds
    titanBaseConfig.setProperty("cache.tx-cache-size", 100)
    titanBaseConfig.setProperty("cache.db-cache", false)

    titanGraph = TitanFactory.open(titanBaseConfig)
    titanIdGraph = setupIdGraph()
    titanGraph.commit()
  }

  def setupIdGraph(): IdGraph[TitanGraph] = {
    val graphManager = titanGraph.getManagementSystem()
    val idKey = graphManager.makePropertyKey(IdGraph.ID).dataType(classOf[java.lang.Long]).make()
    graphManager.buildIndex(IdGraph.ID, classOf[Vertex]).addKey(idKey).buildCompositeIndex()
    graphManager.commit()
    new IdGraph(titanGraph, true, false)

  }

  /**
   * IMPORTANT! removes temporary files
   */
  def cleanupTitan(): Unit = {
    try {
      if (titanGraph != null) {
        titanGraph.shutdown()
      }
    }
    finally {
      DirectoryUtils.deleteTempDirectory(tmpDir)
    }

    // make sure this class is unusable when we're done
    titanBaseConfig = null
    titanGraph = null
    tmpDir = null
    titanIdGraph = null
  }

}
