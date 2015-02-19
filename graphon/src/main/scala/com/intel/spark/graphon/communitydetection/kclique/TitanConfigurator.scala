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

package com.intel.spark.graphon.communitydetection.kclique

import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.typesafe.config.Config

class TitanConfigurator(config: Config) {

  def configure(): SerializableBaseConfiguration = {
    val titanConfigInput = config.getConfig("titan.load")

    val titanConfig = new SerializableBaseConfiguration()
    titanConfig.setProperty("storage.backend", titanConfigInput.getString("storage.backend"))
    titanConfig.setProperty("storage.hostname", titanConfigInput.getString("storage.hostname"))
    titanConfig.setProperty("storage.port", titanConfigInput.getString("storage.port"))
    titanConfig.setProperty("storage.batch-loading", titanConfigInput.getString("storage.batch-loading"))
    titanConfig.setProperty("storage.buffer-size", titanConfigInput.getString("storage.buffer-size"))
    titanConfig.setProperty("storage.attempt-wait", titanConfigInput.getString("storage.attempt-wait"))
    titanConfig.setProperty("storage.lock-wait-time", titanConfigInput.getString("storage.lock-wait-time"))
    titanConfig.setProperty("storage.lock-retries", titanConfigInput.getString("storage.lock-retries"))
    titanConfig.setProperty("storage.idauthority-retries", titanConfigInput.getString("storage.idauthority-retries"))
    titanConfig.setProperty("storage.read-attempts", titanConfigInput.getString("storage.read-attempts"))
    titanConfig.setProperty("autotype", titanConfigInput.getString("autotype"))
    titanConfig.setProperty("ids.block-size", titanConfigInput.getString("ids.block-size"))
    titanConfig.setProperty("ids.renew-timeout", titanConfigInput.getString("ids.renew-timeout"))

    titanConfig
  }

}
