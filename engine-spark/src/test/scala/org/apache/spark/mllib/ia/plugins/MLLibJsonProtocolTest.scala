//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package org.apache.spark.mllib.ia.plugins

//import org.apache.commons.math3.geometry.VectorFormat

import com.intel.intelanalytics.libSvmPlugins.LibSvmData
import libsvm.{ svm_node, svm_parameter, svm_model }
import org.apache.spark.mllib.classification.{ SVMModel, LogisticRegressionModel }
import org.apache.spark.mllib.clustering.KMeansModel
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import org.apache.spark.mllib.ia.plugins.MLLibJsonProtocol._
import org.apache.spark.mllib.ia.plugins.classification.{ SVMData, LogisticRegressionData }
import org.apache.spark.mllib.ia.plugins.clustering.KMeansData
import org.apache.spark.mllib.linalg.{ DenseVector, SparseVector }
import org.scalatest.WordSpec
import spray.json._
import Array._

class MLLibJsonProtocolTest extends WordSpec {
  "DenseVectorFormat" should {

    "be able to serialize" in {
      val dv = new DenseVector(Array(1.2, 3.4, 2.2))
      assert(dv.toJson.compactPrint == "{\"values\":[1.2,3.4,2.2]}")
    }

    "parse json" in {
      val string =
        """
          |{
          |   "values": [1.2,3.4,5.6,7.8]
          |
          |
          |}
        """.stripMargin
      val json = JsonParser(string).asJsObject
      val dv = json.convertTo[DenseVector]
      assert(dv.values.length == 4)
    }
  }

  "SparseVectorFormat" should {

    "be able to serialize" in {
      val sv = new SparseVector(2, Array(1, 2, 3), Array(1.5, 2.5, 3.5))
      assert(sv.toJson.compactPrint == "{\"size\":2,\"indices\":[1,2,3],\"values\":[1.5,2.5,3.5]}")
    }

    "parse json" in {
      val string =
        """
        |{
        |   "size": 3,
        |   "indices": [1,2,3,4],
        |   "values": [1.5,2.5,3.5,4.5]
        |
        |
        | }
      """.
          stripMargin
      val json = JsonParser(string).asJsObject
      val sv = json.convertTo[SparseVector]
      assert(sv.size == 3)
      assert(sv.indices.length == 4)
      assert(sv.values.length == 4)
    }
  }

  "KmeansModelFormat" should {

    "be able to serialize" in {
      val v = new KMeansModel(Array(new DenseVector(Array(1.2, 2.1)), new DenseVector(Array(3.4, 4.3))))
      assert(v.toJson.compactPrint == "{\"clusterCenters\":[{\"values\":[1.2,2.1]},{\"values\":[3.4,4.3]}]}")
    }

    "parse json" in {
      val string = "{\"clusterCenters\":[{\"values\":[1.2,2.1]},{\"values\":[3.4,4.3]}]}"
      val json = JsonParser(string).asJsObject
      val v = json.convertTo[KMeansModel]
      assert(v.clusterCenters.length == 2)
    }
  }

  "KMeansDataFormat" should {

    "be able to serialize" in {
      val d = new KMeansData(new KMeansModel(Array(new DenseVector(Array(1.2, 2.1)),
        new DenseVector(Array(3.4, 4.3)))), List("column1", "column2"), List(1.0, 2.0))
      assert(d.toJson.compactPrint == "{\"k_means_model\":{\"clusterCenters\":[{\"values\":[1.2,2.1]},{\"values\":[3.4,4.3]}]},\"observation_columns\":[\"column1\",\"column2\"],\"column_scalings\":[1.0,2.0]}")
    }

    "parse json" in {
      val string = "{\"k_means_model\":{\"clusterCenters\":[{\"values\":[1.2,2.1]},{\"values\":[3.4,4.3]}]},\"observation_columns\":[\"column1\",\"column2\"],\"column_scalings\":[1.0,2.0]}"
      val json = JsonParser(string).asJsObject
      val d = json.convertTo[KMeansData]
      assert(d.kMeansModel.clusterCenters.length == 2)
      assert(d.observationColumns.length == d.columnScalings.length)
      assert(d.observationColumns.length == 2)
    }
  }

  "LogisticRegressionDataFormat" should {

    "be able to serialize" in {
      val l = new LogisticRegressionData(new LogisticRegressionModel(new DenseVector(Array(1.3, 3.1)), 3.5), List("column1", "column2"))
      assert(l.toJson.compactPrint == "{\"log_reg_model\":{\"weights\":{\"values\":[1.3,3.1]},\"intercept\":3.5},\"observation_columns\":[\"column1\",\"column2\"]}")
    }

    "parse json" in {
      val string = "{\"log_reg_model\":{\"weights\":{\"values\":[1.3,3.1]},\"intercept\":3.5},\"observation_columns\":[\"column1\",\"column2\"]}"
      val json = JsonParser(string).asJsObject
      val l = json.convertTo[LogisticRegressionData]

      assert(l.logRegModel.weights.size == 2)
      assert(l.logRegModel.intercept == 3.5)
      assert(l.observationColumns.length == 2)
    }
  }

  "SVMDataFormat" should {

    "be able to serialize" in {
      val s = new SVMData(new SVMModel(new DenseVector(Array(2.3, 3.4, 4.5)), 3.0), List("column1", "column2", "columns3", "column4"))
      assert(s.toJson.compactPrint == "{\"svm_model\":{\"weights\":{\"values\":[2.3,3.4,4.5]},\"intercept\":3.0},\"observation_columns\":[\"column1\",\"column2\",\"columns3\",\"column4\"]}")
    }

    "parse json" in {
      val string = "{\"svm_model\":{\"weights\":{\"values\":[2.3,3.4,4.5]},\"intercept\":3.0},\"observation_columns\":[\"column1\",\"column2\",\"columns3\",\"column4\"]}"
      val json = JsonParser(string).asJsObject
      val s = json.convertTo[SVMData]

      assert(s.svmModel.weights.size == 3)
      assert(s.svmModel.intercept == 3.0)
      assert(s.observationColumns.length == 4)
    }

  }

  "LibSvmDataFormat" should {

    "be able to serialize" in {
      val s = new LibSvmData(new svm_model(), List("column1", "column2", "columns3", "column4"))

      var myMatrix = ofDim[Double](3, 3)
      for (i <- 0 to 2) {
        for (j <- 0 to 2) {
          myMatrix(i)(j) = j
        }
      }
      val myNode = new svm_node()
      myNode.index = 1
      myNode.value = 3.0

      var myNodeMatrix = ofDim[svm_node](3, 3)
      for (i <- 0 to 2) {
        for (j <- 0 to 2) {
          myNodeMatrix(i)(j) = myNode
        }
      }
      s.svmModel.l = 1
      //s.svmModel.label = Array(1, 3)
      s.svmModel.nr_class = 2
      //s.svmModel.nSV = Array(1, 3, 5)
      s.svmModel.param = BuildParam()
      s.svmModel.SV = myNodeMatrix
      //s.svmModel.probA = Array(1.0, 2.0)
      //s.svmModel.probB = Array(1.0, 2.0, 3.0)
      s.svmModel.rho = Array(1.0, 2.0)
      s.svmModel.sv_coef = myMatrix
      s.svmModel.sv_indices = Array(1, 3)
      assert(s.toJson.compactPrint == "{\"svm_model\":{\"nr_class\":2,\"l\":1,\"rho\":[1.0,2.0],\"sv_indices\":[1,3],\"sv_coef\":[[0.0,1.0,2.0],[0.0,1.0,2.0],[0.0,1.0,2.0]],\"param\":{\"svm_type\":1,\"kernel_type\":2,\"degree\":4,\"gamma\":3.0,\"coef0\":4.0,\"cache_size\":3.0,\"eps\":3.0,\"C\":2.0,\"nr_weight\":1,\"weight_label\":[1,2],\"weight\":[1.0,2.0],\"nu\":1.0,\"p\":2.0,\"shrinking\":1,\"probability\":2},\"SV\":[[{\"index\":1,\"value\":3.0},{\"index\":1,\"value\":3.0},{\"index\":1,\"value\":3.0}],[{\"index\":1,\"value\":3.0},{\"index\":1,\"value\":3.0},{\"index\":1,\"value\":3.0}],[{\"index\":1,\"value\":3.0},{\"index\":1,\"value\":3.0},{\"index\":1,\"value\":3.0}]]},\"observation_columns\":[\"column1\",\"column2\",\"columns3\",\"column4\"]}")
    }

    "parse json" in {
      val string = "{\"svm_model\":{\"nr_class\":2,\"l\":1,\"rho\":[1.0,2.0],\"sv_indices\":[1,3],\"sv_coef\":[[0.0,1.0,2.0],[0.0,1.0,2.0],[0.0,1.0,2.0]],\"param\":{\"svm_type\":1,\"kernel_type\":2,\"degree\":4,\"gamma\":3.0,\"coef0\":4.0,\"cache_size\":3.0,\"eps\":3.0,\"C\":2.0,\"nr_weight\":1,\"weight_label\":[1,2],\"weight\":[1.0,2.0],\"nu\":1.0,\"p\":2.0,\"shrinking\":1,\"probability\":2},\"SV\":[[{\"index\":1,\"value\":3.0},{\"index\":1,\"value\":3.0},{\"index\":1,\"value\":3.0}],[{\"index\":1,\"value\":3.0},{\"index\":1,\"value\":3.0},{\"index\":1,\"value\":3.0}],[{\"index\":1,\"value\":3.0},{\"index\":1,\"value\":3.0},{\"index\":1,\"value\":3.0}]]},\"observation_columns\":[\"column1\",\"column2\",\"columns3\",\"column4\"]}"
      val json = JsonParser(string).asJsObject
      val s = json.convertTo[LibSvmData]

      //assert(s.svmModel.probA.size == 2)
      assert(s.svmModel.l == 1)
      assert(s.observationColumns.length == 4)
      //assert(s.svmModel.label.size == 2)
      assert(s.svmModel.nr_class == 2)
      //assert(s.svmModel.nSV.size == 3)
      //assert(s.svmModel.probB.size == 3)
      assert(s.svmModel.rho.size == 2)
      assert(s.svmModel.sv_indices.size == 2)
      assert(s.svmModel.param.svm_type == 1)
      assert(s.svmModel.param.kernel_type == 2)
      assert(s.svmModel.param.degree == 4)
      assert(s.svmModel.param.gamma == 3.0)
      assert(s.svmModel.param.coef0 == 4.0)
      assert(s.svmModel.param.cache_size == 3.0)
      assert(s.svmModel.param.eps == 3.0)
      assert(s.svmModel.param.C == 2.0)
      assert(s.svmModel.param.nr_weight == 1)
      assert(s.svmModel.param.weight_label.size == 2)
      assert(s.svmModel.param.weight.size == 2)
      assert(s.svmModel.param.nu == 1.0)
      assert(s.svmModel.param.p == 2.0)
      assert(s.svmModel.param.shrinking == 1)
      assert(s.svmModel.param.probability == 2)

    }

  }

  private def BuildParam(): svm_parameter = {

    val param = new svm_parameter()
    param.svm_type = 1
    param.kernel_type = 2
    param.degree = 4
    param.gamma = 3.0
    param.coef0 = 4.0
    param.cache_size = 3.0
    param.eps = 3.0
    param.C = 2.0
    param.nr_weight = 1
    param.weight_label = Array(1, 2)
    param.weight = Array(1.0, 2.0)
    param.nu = 1.0
    param.p = 2.0
    param.shrinking = 1
    param.probability = 2
    param
  }

}

