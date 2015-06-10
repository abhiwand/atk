
package com.intel.intelanalytics.scoring

import java.io._
import java.util.StringTokenizer

import _root_.libsvm.{ svm, svm_model, svm_node }
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.domain.schema.DataTypes
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.DenseVector
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

class LibKMeansModel(libKMeansModel: KMeansModel) extends KMeansModel(libKMeansModel.clusterCenters) with Model {

  override def score(values: String): Future[Double] = future {
    val vector = DataTypes.toVector(-1)(values)
    val output = columnFormatter(vector.toArray.zipWithIndex)

    val splitObs: StringTokenizer = new StringTokenizer(output, " \t\n\r\f:")
    splitObs.nextToken()
    val counter: Int = splitObs.countTokens / 2
    val x: Array[Double] = new Array[Double](counter)
    var j: Int = 0
    while (j < counter) {
      x(j) = atof(splitObs.nextToken)
      j += 1
    }
    predict(new DenseVector(x))
    3.0
  }

  private def columnFormatter(valueIndexPairArray: Array[(Any, Int)]): String = {
    val result = for {
      i <- valueIndexPairArray
      value = i._1
      index = i._2
      if value != 0
    } yield s"$index:$value"
    s"${valueIndexPairArray(0)._1} ${result.mkString(" ")}"
  }

  def atof(s: String): Double = {
    s.toDouble
  }

  def atoi(s: String): Int = {
    Integer.parseInt(s)
  }

}
