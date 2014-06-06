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

package com.intel.graphbuilder.testutils

import DirectoryUtils._
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.thinkaurelius.titan.core.TitanGraph
import java.io.File
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph

/**
 * This trait can be mixed into Specifications to get a TitanGraph backed by Berkeley for testing purposes.
 *
 * IMPORTANT! only one thread can use the graph below at a time. This isn't normally an issue because
 * each test usually gets its own copy.
 */
trait TestingTitan extends MultipleAfter {

  LogUtils.silenceTitan()

  private var tmpDir: File = createTempDirectory("titan-graph-for-unit-testing-")

  var titanConfig = new SerializableBaseConfiguration()
  titanConfig.setProperty("storage.directory", tmpDir.getAbsolutePath)

  var titanConnector = new TitanGraphConnector(titanConfig)
  var graph = titanConnector.connect()

  override def after: Unit = {
    cleanupTitan()
    super.after
  }

  /**
   * IMPORTANT! removes temporary files
   */
  def cleanupTitan(): Unit = {
    try {
      if (graph != null) {
        graph.shutdown()
      }
    }
    finally {
      deleteTempDirectory(tmpDir)
    }

    // make sure this class is unusable when we're done
    titanConfig = null
    titanConnector = null
    graph = null
    tmpDir = null
  }

}
