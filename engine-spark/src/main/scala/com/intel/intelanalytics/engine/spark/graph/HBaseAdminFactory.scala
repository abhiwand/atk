package com.intel.intelanalytics.engine.spark.graph

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
    val hBaseDefault = this.getClass.getClassLoader.getResourceAsStream("hbase-default.xml")
    val hBaseSite = this.getClass.getClassLoader.getResourceAsStream("hbase-site.xml")
    config.addResource(hBaseDefault)
    config.addResource(hBaseSite)

    // Skip check for default hbase version which causes intermittent errors "|hbase-default.xml file seems to be for and old version of HBase (null), this version is 0.98.1-cdh5.1.2|"
    // This error shows up despite setting the correct classpath in bin/api-server.sh and packaging the correct cdh hbase jars
    config.setBoolean("hbase.defaults.for.version.skip", true)
    new HBaseAdmin(HBaseConfiguration.addHbaseResources(config))
  }

}