package com.intel.intelanalytics.engine.spark

import org.scalatest.WordSpec

class CommandDumperConfigTest extends WordSpec {

  "CommandDumperConfig" should {
    "not have null metastore connection information" in {
      assert(CommandDumperConfig.metaStoreConnectionDriver != null)
      assert(CommandDumperConfig.metaStoreConnectionPassword != null)
      assert(CommandDumperConfig.metaStoreConnectionUrl != null)
      assert(CommandDumperConfig.metaStoreConnectionUsername != null)
    }
  }
}
