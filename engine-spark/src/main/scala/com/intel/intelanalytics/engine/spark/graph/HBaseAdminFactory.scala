package com.intel.intelanalytics.engine.spark.graph

import java.io.InputStream

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.conf.Configuration

/**
 * Create HBaseAdmin instances
 *
 * HBaseAdmin should not be re-used forever: you should create, use, throw away - and then get another one next time
 */
class HBaseAdminFactory {

  /**
   * HBaseAdmin should not be re-used forever: you should create, use, throw away - and then get another one next time
   */
  def createHBaseAdmin(): HBaseAdmin = {
    val config = new Configuration()

    // for some reason HBaseConfiguration wasn't picking up hbase-default.xml automatically, so manually adding here
    config.addResource(getClass.getClassLoader.getResourceAsStream("hbase-default.xml"))
    config.addResource(getClass.getClassLoader.getResourceAsStream("hbase-site.xml"))

    // Skip check for default hbase version which causes intermittent errors "|hbase-default.xml file seems to be for and old version of HBase (null), this version is 0.98.1-cdh5.1.2|"
    // This error shows up despite setting the correct classpath in bin/api-server.sh and packaging the correct cdh hbase jars
    config.setBoolean("hbase.defaults.for.version.skip", true)

    // To help with debugging
    println("HbaseAdmin Configuration")
    config.writeXml(System.out)

    // validation from HBaseConfiguration that we might want
    checkForClusterFreeMemoryLimit(config)

    // HBaseConfiguration.addHbaseResources(config) doesn't seem to work with this version of HBase, maybe because of how we load classes using Boot.scala
    new HBaseAdmin(config)
  }

  // TODO: This method was copied from Open Source HBaseConfiguration.java, include copyright / license info
  private def checkForClusterFreeMemoryLimit(conf: Configuration): Unit = {
    val globalMemstoreLimit = conf.getFloat("hbase.regionserver.global.memstore.upperLimit", 0.4F)
    val gml = (globalMemstoreLimit * 100.0F).toInt
    val blockCacheUpperLimit = conf.getFloat("hfile.block.cache.size", 0.25F)

    val bcul = (blockCacheUpperLimit * 100.0F).toInt
    if (100 - (gml + bcul) < 20) {
      throw new RuntimeException("Current heap configuration for MemStore and BlockCache exceeds the threshold required for successful cluster operation. The combined value cannot exceed 0.8. Please check the settings for hbase.regionserver.global.memstore.upperLimit and hfile.block.cache.size in your configuration. hbase.regionserver.global.memstore.upperLimit is " + globalMemstoreLimit + " hfile.block.cache.size is " + blockCacheUpperLimit);
    }
  }

}
