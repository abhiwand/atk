package com.intel.spark.mllib.classification

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Main {
  def main(args: Array[String]) {
    val sparkHome = "/root/spark"
    val jarFile = "target/scala-2.10/imllib_2.10-0.0.jar"
    val sc = new SparkContext("local", "TestJob", sparkHome, Seq(jarFile))
    println("1+2+...+10 = " + sc.parallelize(1 to 10).reduce(_ + _))
  }
}

