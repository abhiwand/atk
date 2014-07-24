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
