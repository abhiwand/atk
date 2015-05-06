package com.intel.intelanalytics.scoring

import java.io._
import java.util.StringTokenizer

import _root_.libsvm.{ svm, svm_model, svm_node }
import com.intel.intelanalytics.domain.schema.DataTypes

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

class LibSvmModel(libSvmModel: svm_model) extends svm_model with Model {

  override def score(values: String): Future[Double] = future {
    val vector = DataTypes.toVector(-1)(values)
    val output = columnFormatter(vector.toArray.zipWithIndex)

    val splitObs: StringTokenizer = new StringTokenizer(output, " \t\n\r\f:")
    splitObs.nextToken()
    val counter: Int = splitObs.countTokens / 2
    val x: Array[svm_node] = new Array[svm_node](counter)
    var j: Int = 0
    while (j < counter) {
      x(j) = new svm_node
      x(j).index = atoi(splitObs.nextToken) + 1
      x(j).value = atof(splitObs.nextToken)
      j += 1
    }
    svm.svm_predict(libSvmModel, x)
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
