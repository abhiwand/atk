package com.intel.graphbuilder.driver.spark.titan.examples

import java.util.Date
import org.apache.spark.SparkContext

/**
 * This utility is for testing that Spark can read a file from HDFS.
 *
 * Helpful when troubleshooting if Spark is working correctly.
 */
object SparkReadFileTest {

  def main(args: Array[String]): Unit = {

    val appName = this.getClass.getSimpleName + " " + new Date()

    val sc = new SparkContext(ExamplesUtils.sparkMaster, appName)

    println("Trying to read file: " + ExamplesUtils.movieDataset)

    val inputRows = sc.textFile(ExamplesUtils.movieDataset, System.getProperty("PARTITIONS", "120").toInt)
    val inputRdd = inputRows.map(row => row.split(","): Seq[_])

    println("Spark Read Input Rows: " + inputRdd.count())
  }
}
