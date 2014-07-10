package com.intel.intelanalytics.engine.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

class DiscreteStatistics[T](data: RDD[T]) {

  private lazy val mode: T = generateMode()

  private def generateMode(): T = {

    // the countByValue operation creates (value, count) pairs out the RDD
    // hence the mode is the first component of the component with maximum second component

    def takeValueWithMaximumCount(p1: (T, Long), p2: (T, Long)): (T, Long) = {
      if (p1._2 > p2._2) p1 else p2
    }
    data.countByValue().reduce(takeValueWithMaximumCount)._1
  }
}