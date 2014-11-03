package com.intel.graphbuilder.graph.titan

import org.apache.commons.configuration.BaseConfiguration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ FlatSpec, Matchers }
import org.mockito.Mockito._

class TitanAutoPartitionerTest extends FlatSpec with Matchers with MockitoSugar {

  "enableAutoPartition" should "return true when auto-partitioner is enabled" in {
    val titanConfig = new BaseConfiguration()
    titanConfig.setProperty(TitanAutoPartitioner.ENABLE_AUTO_PARTITION, "true")

    val titanAutoPartitioner = new TitanAutoPartitioner(titanConfig)
    titanAutoPartitioner.enableAutoPartition shouldBe (true)
  }

  "enableAutoPartition" should "return false when auto-partitioner is disabled" in {
    val titanConfig = new BaseConfiguration()
    titanConfig.setProperty(TitanAutoPartitioner.ENABLE_AUTO_PARTITION, "false")

    val titanAutoPartitioner = new TitanAutoPartitioner(titanConfig)
    titanAutoPartitioner.enableAutoPartition shouldBe (false)
  }

  /*"getInputSplits" should "dd" in {
    val titanConfig = new BaseConfiguration()
    val hBaseConfig = HBaseConfiguration.create()
    val hbaseAdmin = new HBaseAdmin(hBaseConfig)
    titanConfig.setProperty(TitanAutoPartitioner.ENABLE_AUTO_PARTITION, "true")
    titanConfig.setProperty(TitanAutoPartitioner.HBASE_INPUT_SPLITS_PER_CORE, "2")

    val titanAutoPartitioner = mock[TitanAutoPartitioner]
    when(titanAutoPartitioner.titanConfig).thenReturn(titanConfig)
    when(titanAutoPartitioner.getHBaseRegionServerCount(hbaseAdmin)).thenReturn(4)

    titanAutoPartitioner.setHBasePreSplits(hBaseConfig)
    println(titanAutoPartitioner)
  } */
}
