package com.intel.graphbuilder.driver.spark.titan.examples

import java.util.Date
import org.apache.spark.SparkContext

/**
 * Test if you can connect to Spark and do something trivial
 */
object SparkTest {

  val master = "spark://GAO-WSE.jf.intel.com:7077"

  def main(args: Array[String]): Unit = {

    val appName = this.getClass.getSimpleName + " " + new Date()

    val sc = new SparkContext(master, appName)

    val count = sc.parallelize(1 to 100).count()

    println("count: " + count)
    println("Spark is working: " + (count == 100))
  }
}
