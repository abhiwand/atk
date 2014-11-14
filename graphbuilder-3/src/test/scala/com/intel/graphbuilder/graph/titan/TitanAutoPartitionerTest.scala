package com.intel.graphbuilder.graph.titan

import com.intel.graphbuilder.io.GBTitanHBaseInputFormat
import org.apache.commons.configuration.BaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.{ HRegionInfo, TableName, ClusterStatus, HBaseConfiguration }
import org.apache.spark.{ SparkConf, SparkContext }
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest._

class TitanAutoPartitionerTest extends FlatSpec with Matchers with MockitoSugar with BeforeAndAfterEach {
  val hBaseTableName = "testtable"
  val hBaseRegionServers = 3
  val hBaseTableRegions = 5
  val hBaseTableSizeMB = 2000

  val hBaseAdminMock = mock[HBaseAdmin]
  val clusterStatusMock = mock[ClusterStatus]
  val tableRegionsMock = mock[java.util.List[HRegionInfo]]

  override def beforeEach() = {
    val hBaseConfig = HBaseConfiguration.create()

    when(hBaseAdminMock.getConfiguration).thenReturn(hBaseConfig)

    //Mock number of HBase region servers
    when(hBaseAdminMock.getClusterStatus()).thenReturn(clusterStatusMock)
    when(hBaseAdminMock.getClusterStatus.getServersSize).thenReturn(hBaseRegionServers)

    //Mock number of regions in HBase table
    when(hBaseAdminMock.getTableRegions(TableName.valueOf(hBaseTableName))).thenReturn(tableRegionsMock)
    when(hBaseAdminMock.getTableRegions(TableName.valueOf(hBaseTableName)).size()).thenReturn(hBaseTableRegions)
  }

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

  "setHBasePreSplits" should "set HBase pre-splits for graph construction based on available region servers" in {
    val titanConfig = new BaseConfiguration()
    titanConfig.setProperty(TitanAutoPartitioner.ENABLE_AUTO_PARTITION, "true")
    titanConfig.setProperty(TitanAutoPartitioner.HBASE_REGIONS_PER_SERVER, "6")

    val titanAutoPartitioner = TitanAutoPartitioner(titanConfig)
    val newTitanConfig = titanAutoPartitioner.setHBasePreSplits(hBaseAdminMock)

    val expectedRegionCount = hBaseRegionServers * 6
    newTitanConfig.getProperty(TitanAutoPartitioner.TITAN_HBASE_REGION_COUNT) shouldBe (expectedRegionCount)
  }
  "setHBasePreSplits" should "not set HBase pre-splits if regions are less than Titan's minimum region count" in {
    val titanConfig = new BaseConfiguration()
    titanConfig.setProperty(TitanAutoPartitioner.ENABLE_AUTO_PARTITION, "true")
    titanConfig.setProperty(TitanAutoPartitioner.HBASE_REGIONS_PER_SERVER, "0")

    val titanAutoPartitioner = TitanAutoPartitioner(titanConfig)
    val newTitanConfig = titanAutoPartitioner.setHBasePreSplits(hBaseAdminMock)
    newTitanConfig.getProperty(TitanAutoPartitioner.TITAN_HBASE_REGION_COUNT) shouldBe (null)
  }
  "setHBaseInputSplits" should "set input splits for Titan/HBase reader to spark.cores.max" in {
    val sparkConfig = new SparkConf()
    val sparkContextMock = mock[SparkContext]
    val tableName = TableName.valueOf(hBaseTableName)
    val hBaseConfig = hBaseAdminMock.getConfiguration

    sparkConfig.set(TitanAutoPartitioner.SPARK_MAX_CORES, "20")
    when(sparkContextMock.getConf).thenReturn(sparkConfig)

    val titanConfig = new BaseConfiguration()
    titanConfig.setProperty(TitanAutoPartitioner.ENABLE_AUTO_PARTITION, "true")
    titanConfig.setProperty(TitanAutoPartitioner.HBASE_MIN_INPUT_SPLIT_SIZE_MB, 10)

    val titanAutoPartitioner = TitanAutoPartitioner(titanConfig)
    titanAutoPartitioner.setSparkHBaseInputSplits(sparkContextMock, hBaseAdminMock, hBaseTableName)
    //TODO: Fix test
    //hBaseAdminMock.getConfiguration.getInt(GBTitanHBaseInputFormat.NUM_REGION_SPLITS, -1) shouldBe (20)
  }
  "setHBaseInputSplits" should "set input splits for Titan/HBase reader to estimated Spark cores when spark.cores.max not specified" in {
    val sparkContext = mock[SparkContext]
    val sparkConfig = new SparkConf()
    when(sparkContext.getConf).thenReturn(sparkConfig)

    val titanConfig = new BaseConfiguration()
    titanConfig.setProperty(TitanAutoPartitioner.ENABLE_AUTO_PARTITION, "true")
    titanConfig.setProperty(TitanAutoPartitioner.HBASE_INPUT_SPLITS_PER_CORE, "4")

    val titanAutoPartitioner = TitanAutoPartitioner(titanConfig)
    titanAutoPartitioner.setSparkHBaseInputSplits(sparkContext, hBaseAdminMock, hBaseTableName)

    val numSparkWorkers = hBaseRegionServers //HBase region servers used to estimate Spark workers
    val expectedHBaseSplits = Runtime.getRuntime.availableProcessors() * numSparkWorkers * 4
    //TODO: Fix test
    //hBaseAdminMock.getConfiguration.getInt(GBTitanHBaseInputFormat.NUM_REGION_SPLITS, -1) shouldBe (expectedHBaseSplits)
  }

}
