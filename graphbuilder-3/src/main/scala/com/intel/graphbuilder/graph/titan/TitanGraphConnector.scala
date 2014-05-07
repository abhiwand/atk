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

package com.intel.graphbuilder.graph.titan

import com.intel.graphbuilder.graph.GraphConnector
import com.thinkaurelius.titan.core.{ TitanGraph, TitanFactory }
import java.io.File
import org.apache.commons.configuration.{ PropertiesConfiguration, Configuration }

/**
 * Get a connection to Titan.
 * <p>
 * This was needed because a 'Connector' can be serialized across the network, whereas a live connection can't.
 * </p>
 */
class TitanGraphConnector(config: Configuration) extends GraphConnector with Serializable {

  /**
   * Initialize using a properties configuration file.
   * @param propertiesFile a .properties file, see example-titan.properties and Titan docs
   */
  def this(propertiesFile: File) {
    this(new PropertiesConfiguration(propertiesFile))
  }

  /**
   * Get a connection to a graph database
   */
  override def connect(): TitanGraph = {
    TitanFactory.open(config)
  }

}
