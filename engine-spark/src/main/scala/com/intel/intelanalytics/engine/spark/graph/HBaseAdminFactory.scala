package com.intel.intelanalytics.engine.spark.graph

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin

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
    new HBaseAdmin(HBaseConfiguration.create())
  }

}
