package com.intel.intelanalytics.engine.spark.graph

import java.io.File

import com.intel.graphbuilder.driver.spark.rdd.EnvironmentValidator
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.testutils.{ TestingTitan, TestingSparkContextWordSpec, DirectoryUtils, LogUtils }
import com.thinkaurelius.titan.core.util.TitanCleanup
import com.thinkaurelius.titan.core.{ TitanFactory, TitanGraph }
import com.tinkerpop.blueprints.Graph
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph
import org.scalatest.BeforeAndAfter

trait TestingTitanWithSparkWordSpec extends TestingSparkContextWordSpec with BeforeAndAfter with TestingTitan {
  LogUtils.silenceTitan()

  var graph: Graph = null
  val titanConfig = new SerializableBaseConfiguration()

  override def beforeAll() {
    EnvironmentValidator.skipEnvironmentValidation = true
    super[TestingSparkContextWordSpec].beforeAll()
    setupTitan()
    titanConfig.copy(titanBaseConfig)
  }

  override def afterAll() {
    super[TestingSparkContextWordSpec].afterAll()
    cleanupTitan()
  }

}
