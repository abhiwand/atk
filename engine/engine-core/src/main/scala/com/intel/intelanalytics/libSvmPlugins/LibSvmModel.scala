package com.intel.intelanalytics.libSvmPlugins

import _root_.libsvm.{ svm, svm_model, svm_node }
import com.intel.intelanalytics.domain.schema.DataTypes

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import com.intel.intelanalytics.interfaces.Model
import java.util.StringTokenizer

class LibSvmModel(libSvmModel: svm_model) extends svm_model with Model {

  private var _name = ""
  override def name = _name
  def name_=(value: String): Unit = _name = value

  override def score(data: Seq[Array[Any]]): Future[Seq[Any]] = future {
    val vector = DataTypes.toVector(-1)(data.mkString(","))
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
    val score = svm.svm_predict(libSvmModel, x)
    Seq(score)
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
