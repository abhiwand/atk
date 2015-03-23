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
import libsvm.svm_model
import org.apache.spark.mllib.classification.{ SVMModel, LogisticRegressionModel }
import org.apache.spark.mllib.clustering.KMeansModel
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import org.apache.spark.mllib.ia.plugins.MLLibJsonProtocol._
import org.apache.spark.mllib.ia.plugins.classification.{ SVMData, LogisticRegressionData }
import org.apache.spark.mllib.ia.plugins.clustering.KMeansData
import org.apache.spark.mllib.linalg.{ DenseVector, SparseVector }
import org.scalatest.WordSpec
import spray.json._

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
      val s = new LibSvmData(new svm_model(new DenseVector(Array(2.3, 3.4, 4.5)), 3.0), List("column1", "column2", "columns3", "column4"))
      assert(s.toJson.compactPrint == "{\"svm_model\":{\"weights\":{\"values\":[2.3,3.4,4.5]},\"intercept\":3.0},\"observation_columns\":[\"column1\",\"column2\",\"columns3\",\"column4\"]}")
    }

    "parse json" in {
      val string = "{\"svm_model\":{\"weights\":{\"values\":[2.3,3.4,4.5]},\"intercept\":3.0},\"observation_columns\":[\"column1\",\"column2\",\"columns3\",\"column4\"]}"
      val json = JsonParser(string).asJsObject
      val s = json.convertTo[LibSvmData]

      assert(s.svmModel.weights.size == 3)
      assert(s.svmModel.intercept == 3.0)
      assert(s.observationColumns.length == 4)
    }
  }
}

//public libsvm.svm_parameter param;
//public int nr_class;
//public int l;
//public libsvm.svm_node[][] SV;
//public double[][] sv_coef;
//public double[] rho;
//public double[] probA;
//public double[] probB;
//public int[] sv_indices;
//public int[] label;
//public int[] nSV;
