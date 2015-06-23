package org.apache.spark.mllib.regression

import scala.beans.BeanInfo

import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.util.NumericParser
import org.apache.spark.SparkException

/**
 * Class that represents the features and labels of a data point.
 *
 * @param label Label for this data point.
 * @param features List of features for this data point.
 */
@BeanInfo
case class LabeledPointWithFrequency(label: Double, features: Vector, frequency: Double) {
  override def toString: String = {
    s"($label,$features,$frequency)"
  }
}

/**
 * Parser for [[org.apache.spark.mllib.regression.LabeledPointWithFrequency]].
 */
object LabeledPointWithFrequency {
  /**
   * Parses a string resulted from `LabeledPointWithFrequency#toString` into
   * an [[org.apache.spark.mllib.regression.LabeledPointWithFrequency]].
   */
  def parse(s: String): LabeledPointWithFrequency = {
    if (s.startsWith("(")) {
      NumericParser.parse(s) match {
        case Seq(label: Double, numeric: Any, frequency: Double) =>
          LabeledPointWithFrequency(label, Vectors.parseNumeric(numeric), frequency)
        case other =>
          throw new SparkException(s"Cannot parse $other.")
      }
    } else { // dense format used before v1.0
    val parts = s.split(',')
      val label = java.lang.Double.parseDouble(parts(0))
      val features = Vectors.dense(parts(1).trim().split(' ').map(java.lang.Double.parseDouble))
      val frequency = java.lang.Double.parseDouble(parts(2))
      LabeledPointWithFrequency(label, features, frequency)
    }
  }
}
