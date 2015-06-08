/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.intelanalytics.scoring

import java.io._
import java.util.StringTokenizer

import _root_.libsvm.{ svm, svm_model, svm_node }
import com.intel.intelanalytics.domain.schema.DataTypes

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

class LibSvmModel(libSvmModel: svm_model) extends svm_model with Model {

  private var _name = ""
  override def name = _name
  def name_=(value: String): Unit = _name = value

  override def score(data: Seq[Any]): Future[Seq[Any]] = future {
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
