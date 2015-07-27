package com.intel.taproot.analytics.engine.frame.plugins.join

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

/**
 * Functions for joining pair RDDs using broadcast variables
 */
class BroadcastJoinRddFunctions(self: RddJoinParam) extends Logging with Serializable {

  /**
   * Perform left outer-join using a broadcast variable
   *
   * @param other join parameter for second data frame
   *
   * @return key-value RDD whose values are results of left-outer join
   */
  def leftBroadcastJoin(other: RddJoinParam): RDD[(Any, (Row, Option[Row]))] = {
    //Use multi-map to handle duplicate keys
    val rightBroadcastVariable = JoinBroadcastVariable(other)

    self.rdd.flatMap {
      case (leftKey, leftValues) => {
        rightBroadcastVariable.get(leftKey) match {
          case Some(rightValueSet) => for (values <- rightValueSet) yield (leftKey, (leftValues, Some(values)))
          case _ => List((leftKey, (leftValues, None))).asInstanceOf[List[(Any, (Row, Option[Row]))]]
        }
      }
    }
  }

  /**
   * Right outer-join using a broadcast variable
   *
   * @param other join parameter for second data frame
   *
   * @return key-value RDD whose values are results of right-outer join
   */
  def rightBroadcastJoin(other: RddJoinParam): RDD[(Any, (Option[Row], Row))] = {
    //Use multi-map to handle duplicate keys
    val leftBroadcastVariable = JoinBroadcastVariable(self)

    other.rdd.flatMap {
      case (rightKey, rightValues) => {
        leftBroadcastVariable.get(rightKey) match {
          case Some(leftValueSet) => for (values <- leftValueSet) yield (rightKey, (Some(values), rightValues))
          case _ => List((rightKey, (None, rightValues))).asInstanceOf[List[(Any, (Option[Row], Row))]]
        }
      }
    }
  }

  /**
   * Inner-join using a broadcast variable
   *
   * @param other join parameter for second data frame
   *
   * @return key-value RDD whose values are results of inner-outer join
   */
  def innerBroadcastJoin(other: RddJoinParam, broadcastJoinThreshold: Long): RDD[(Any, (Row, Row))] = {
    val leftSizeInBytes = self.estimatedSizeInBytes.getOrElse(Long.MaxValue)
    val rightSizeInBytes = other.estimatedSizeInBytes.getOrElse(Long.MaxValue)

    val innerJoinedRDD = if (rightSizeInBytes <= broadcastJoinThreshold) {
      //Use multi-map to handle duplicate keys
      val rightBroadcastVariable = JoinBroadcastVariable(other)
      self.rdd.flatMap {
        case (leftKey, leftValues) => {
          val rightValueList = rightBroadcastVariable.get(leftKey).toList
          rightValueList.flatMap(rightValues => {
            for (values <- rightValues) yield (leftKey, (leftValues, values))
          })
        }
      }
    }
    else if (leftSizeInBytes <= broadcastJoinThreshold) {
      //Use multi-map to handle duplicate keys
      val leftBroadcastVariable = JoinBroadcastVariable(self)
      other.rdd.flatMap {
        case (rightKey, rightValues) => {
          val leftValueList = leftBroadcastVariable.get(rightKey).toList
          leftValueList.flatMap(leftValues => {
            for (values <- leftValues) yield (rightKey, (values, rightValues))
          })
        }
      }
    }
    else throw new IllegalArgumentException(s"Frame size exceeds broadcast-join-threshold: ${broadcastJoinThreshold}.")
    innerJoinedRDD
  }
}
