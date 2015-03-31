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

import com.intel.intelanalytics.spray.json.IADefaultJsonProtocol
import libsvm.{ svm_parameter, svm_model }
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.ia.plugins.classification._
import org.apache.spark.mllib.ia.plugins.clustering.{ KMeansPredictArgs, KMeansTrainArgs, KMeansTrainReturn, KMeansData }
import org.apache.spark.mllib.linalg.{ DenseVector, SparseVector, Vector }
import com.intel.intelanalytics.libSvmPlugins._
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Implicit conversions for Logistic Regression objects to/from JSON
 */

object MLLibJsonProtocol {

  implicit object SparseVectorFormat extends JsonFormat[SparseVector] {
    /**
     * Conversion from MLLib's SparseVector format to JsValue
     * @param obj: SparseVector whose format is SparseVector(val size: Int, val indices: Array[Int], val values: Array[Double])
     * @return JsValue
     */
    override def write(obj: SparseVector): JsValue = {
      JsObject(
        "size" -> JsNumber(obj.size),
        "indices" -> new JsArray(obj.indices.map(i => JsNumber(i)).toList),
        "values" -> new JsArray(obj.values.map(d => JsNumber(d)).toList)
      )
    }

    /**
     * Conversion from JsValue to MLLib's SparseVector format
     * @param json: JsValue
     * @return SparseVector whose format is SparseVector(val size: Int, val indices: Array[Int], val values: Array[Double])
     */
    override def read(json: JsValue): SparseVector = {
      val fields = json.asJsObject.fields
      val size = fields.get("size").get.asInstanceOf[JsNumber].value.intValue
      val indices = fields.get("indices").get.asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.intValue).toArray
      val values = fields.get("values").get.asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.doubleValue).toArray

      new SparseVector(size, indices, values)
    }
  }

  implicit object DenseVectorFormat extends JsonFormat[DenseVector] {
    /**
     * Conversion from MLLib's DenseVector format to JsValue
     * @param obj DenseVector, whose format is DenseVector(val values: Array[Double])
     * @return JsValue
     */
    override def write(obj: DenseVector): JsValue = {
      JsObject(
        "values" -> new JsArray(obj.values.map(d => JsNumber(d)).toList)
      )
    }
    /**
     * Conversion from JsValue to MLLib's DenseVector format
     * @param json JsValue
     * @return DenseVector, whose format is DenseVector(val values: Array[Double])
     */
    override def read(json: JsValue): DenseVector = {
      val fields = json.asJsObject.fields
      val values = fields.get("values").get.asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.doubleValue).toArray
      new DenseVector(values)
    }
  }

  implicit object LogisticRegressionModelFormat extends JsonFormat[LogisticRegressionModel] {
    /**
     * The write methods converts from LogisticRegressionModel to JsValue
     * @param obj LogisticRegressionModel. Where LogisticRegressionModel's format is
     *            LogisticRegressionModel(val weights: Vector,val intercept: Double)
     *            and the weights Vector could be either a SparseVector or DenseVector
     * @return JsValue
     */
    override def write(obj: LogisticRegressionModel): JsValue = {
      val weights = VectorFormat.write(obj.weights)
      JsObject(
        "weights" -> weights,
        "intercept" -> JsNumber(obj.intercept)
      )
    }

    /**
     * The read method reads a JsValue to LogisticRegressionModel
     * @param json JsValue
     * @return LogisticRegressionModel with format LogisticRegressionModel(val weights: Vector,val intercept: Double)
     *         and the weights Vector could be either a SparseVector or DenseVector
     */
    override def read(json: JsValue): LogisticRegressionModel = {
      val fields = json.asJsObject.fields
      val intercept = fields.get("intercept").getOrElse(throw new IllegalArgumentException("Error in de-serialization: Missing intercept.")).asInstanceOf[JsNumber].value.doubleValue()

      val weights = fields.get("weights").map(v => {
        VectorFormat.read(v)
      }
      ).get

      new LogisticRegressionModel(weights, intercept)
    }

  }
  implicit object VectorFormat extends JsonFormat[Vector] {
    override def write(obj: Vector): JsValue = {
      obj match {
        case sv: SparseVector => SparseVectorFormat.write(sv)
        case dv: DenseVector => DenseVectorFormat.write(dv)
        case _ => throw new IllegalArgumentException("Object does not confirm to Vector format.")
      }
    }

    override def read(json: JsValue): Vector = {
      if (json.asJsObject.fields.get("size").isDefined) {
        SparseVectorFormat.read(json)
      }
      else {
        DenseVectorFormat.read(json)
      }
    }
  }

  implicit object KmeansModelFormat extends JsonFormat[KMeansModel] {
    /**
     * The write methods converts from KMeans to JsValue
     * @param obj KMeansModel. Where KMeansModel's format is
     *            val clusterCenters: Array[Vector]
     *            and the weights Vector could be either a SparseVector or DenseVector
     * @return JsValue
     */
    override def write(obj: KMeansModel): JsValue = {
      val centers = obj.clusterCenters.map(vector => VectorFormat.write(vector))
      JsObject("clusterCenters" -> JsArray(centers.toList))
    }

    /**
     * The read method reads a JsValue to KMeansModel
     * @param json JsValue
     * @return KMeansModel with format KMeansModel(val clusterCenters:Array[Vector])
     *         where Vector could be either a SparseVector or DenseVector
     */
    override def read(json: JsValue): KMeansModel = {
      val fields = json.asJsObject.fields

      val centers = fields.get("clusterCenters").get.asInstanceOf[JsArray].elements.map(vector => {
        VectorFormat.read(vector)
      })

      new KMeansModel(centers.toArray)
    }

  }

  implicit object SVMModelFormat extends JsonFormat[SVMModel] {
    /**
     * The write methods converts from SVMModel to JsValue
     * @param obj SVMModel. Where SVMModel's format is
     *            SVMModel(val weights: Vector,val intercept: Double)
     *            and the weights Vector could be either a SparseVector or DenseVector
     * @return JsValue
     */
    override def write(obj: SVMModel): JsValue = {
      val weights = VectorFormat.write(obj.weights)
      JsObject(
        "weights" -> weights,
        "intercept" -> JsNumber(obj.intercept)
      )
    }

    /**
     * The read method reads a JsValue to SVMModel
     * @param json JsValue
     * @return LogisticRegressionModel with format SVMModel(val weights: Vector,val intercept: Double)
     *         and the weights Vector could be either a SparseVector or DenseVector
     */
    override def read(json: JsValue): SVMModel = {
      val fields = json.asJsObject.fields
      val intercept = fields.get("intercept").getOrElse(throw new IllegalArgumentException("Error in de-serialization: Missing intercept.")).asInstanceOf[JsNumber].value.doubleValue()

      val weights = fields.get("weights").map(v => {
        VectorFormat.read(v)
      }
      ).get

      new SVMModel(weights, intercept)
    }

  }

  implicit object svm_parameter extends JsonFormat[libsvm.svm_parameter] {
    override def write(obj: libsvm.svm_parameter): JsValue = {
      JsObject(
        "svm_type" -> JsNumber(obj.svm_type),
        "kernel_type" -> JsNumber(obj.kernel_type),
        "degree" -> JsNumber(obj.degree),
        "gamma" -> JsNumber(obj.gamma),
        "coef0" -> JsNumber(obj.coef0),
        "cache_size" -> JsNumber(obj.cache_size),
        "eps" -> JsNumber(obj.eps),
        "C" -> JsNumber(obj.C),
        "nr_weight" -> JsNumber(obj.nr_weight),
        "weight_label" -> new JsArray(obj.weight_label.map(i => JsNumber(i)).toList),
        "weight" -> new JsArray(obj.weight.map(i => JsNumber(i)).toList),
        "nu" -> JsNumber(obj.nu),
        "p" -> JsNumber(obj.p),
        "shrinking" -> JsNumber(obj.shrinking),
        "probability" -> JsNumber(obj.probability)
      )
    }

    override def read(json: JsValue): libsvm.svm_parameter = {
      val fields = json.asJsObject.fields
      val svm_type = fields.get("svm_type").get.asInstanceOf[JsNumber].value.intValue()
      val kernel_type = fields.get("kernel_type").get.asInstanceOf[JsNumber].value.intValue()
      val degree = fields.get("degree").get.asInstanceOf[JsNumber].value.intValue()
      val gamma = fields.get("gamma").get.asInstanceOf[JsNumber].value.doubleValue()
      val coef0 = fields.get("coef0").get.asInstanceOf[JsNumber].value.doubleValue()
      val cache_size = fields.get("cache_size").get.asInstanceOf[JsNumber].value.doubleValue()
      val eps = fields.get("eps").get.asInstanceOf[JsNumber].value.doubleValue()
      val C = fields.get("C").get.asInstanceOf[JsNumber].value.doubleValue()
      val nr_weight = fields.get("nr_weight").get.asInstanceOf[JsNumber].value.intValue()
      val nu = fields.get("nu").get.asInstanceOf[JsNumber].value.doubleValue()
      val p = fields.get("p").get.asInstanceOf[JsNumber].value.doubleValue()
      val shrinking = fields.get("shrinking").get.asInstanceOf[JsNumber].value.intValue()
      val probability = fields.get("probability").get.asInstanceOf[JsNumber].value.intValue()
      val weight_label = fields.get("weight_label").get.asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.intValue()).toArray
      val weight = fields.get("weight").get.asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.doubleValue()).toArray

      val svmParam = new libsvm.svm_parameter()
      svmParam.svm_type = svm_type
      svmParam.kernel_type = kernel_type
      svmParam.degree = degree
      svmParam.gamma = gamma
      svmParam.coef0 = coef0
      svmParam.cache_size = cache_size
      svmParam.eps = eps
      svmParam.C = C
      svmParam.nr_weight = nr_weight
      svmParam.nu = nu
      svmParam.p = p
      svmParam.shrinking = shrinking
      svmParam.probability = probability
      svmParam.weight = weight
      svmParam.weight_label = weight_label

      svmParam
    }
  }

  implicit object svm_node extends JsonFormat[libsvm.svm_node] {
    override def write(obj: libsvm.svm_node): JsValue = {
      JsObject(
        "index" -> JsNumber(obj.index),
        "value" -> JsNumber(obj.value)
      )
    }

    override def read(json: JsValue): libsvm.svm_node = {
      val fields = json.asJsObject.fields
      val index = fields.get("index").get.asInstanceOf[JsNumber].value.intValue()
      val value = fields.get("value").get.asInstanceOf[JsNumber].value.doubleValue()

      val svmNode = new libsvm.svm_node()
      svmNode.index = index
      svmNode.value = value

      svmNode
    }
  }

  implicit object LibSVMModelFormat extends JsonFormat[svm_model] {
    /**
     * The write methods converts from LibSVMModel to JsValue
     * @param obj svm_model
     * @return JsValue
     */
    override def write(obj: svm_model): JsValue = {
      JsObject(
        "nr_class" -> JsNumber(obj.nr_class),
        "l" -> JsNumber(obj.l),
        "rho" -> new JsArray(obj.rho.map(i => JsNumber(i)).toList),
        //"probA" -> new JsArray(obj.probA.map(d => JsNumber(d)).toList),
        //"probB" -> new JsArray(obj.probB.map(d => JsNumber(d)).toList),
        //"label" -> new JsArray(obj.label.map(i => JsNumber(i)).toList),
        "sv_indices" -> new JsArray(obj.sv_indices.map(d => JsNumber(d)).toList),
        "sv_coef" -> new JsArray(obj.sv_coef.map(row => new JsArray(row.map(d => JsNumber(d)).toList)).toList),
        //"nSV" -> new JsArray(obj.nSV.map(i => JsNumber(i)).toList),
        "param" -> svm_parameter.write(obj.param),
        "SV" -> new JsArray(obj.SV.map(row => new JsArray(row.map(d => svm_node.write(d)).toList)).toList)
      )
    }

    /**
     * The read method reads a JsValue to LibSVMModel
     * @param json JsValue
     * @return LogisticRegressionModel with format SVMModel(val weights: Vector,val intercept: Double)
     *         and the weights Vector could be either a SparseVector or DenseVector
     */
    override def read(json: JsValue): svm_model = {
      val fields = json.asJsObject.fields
      val l = fields.get("l").get.asInstanceOf[JsNumber].value.intValue()
      val nr_class = fields.get("nr_class").get.asInstanceOf[JsNumber].value.intValue()
      val rho = fields.get("rho").get.asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.doubleValue()).toArray
      //val probA = fields.get("probA").get.asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.doubleValue()).toArray
      //val probB = fields.get("probB").get.asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.doubleValue()).toArray
      val sv_indices = fields.get("sv_indices").get.asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.intValue()).toArray
      val sv_coef = fields.get("sv_coef").get.asInstanceOf[JsArray].elements.map(row => row.asInstanceOf[JsArray].elements.map(j => j.asInstanceOf[JsNumber].value.doubleValue()).toArray).toArray
      //val label = fields.get("label").get.asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.intValue()).toArray
      //val nSV = fields.get("nSV").get.asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.intValue()).toArray
      val param = fields.get("param").map(v => svm_parameter.read(v)).get
      val SV = fields.get("SV").get.asInstanceOf[JsArray].elements.map(row => row.asInstanceOf[JsArray].elements.map(j => svm_node.read(j))toArray).toArray

      val svmModel = new svm_model()
      svmModel.l = l
      svmModel.nr_class = nr_class
      svmModel.rho = rho
      //svmModel.probA = probA
      //svmModel.probB = probB
      svmModel.sv_indices = sv_indices
      svmModel.sv_coef = sv_coef
      //svmModel.label = label
      //svmModel.nSV = nSV
      svmModel.param = param
      svmModel.SV = SV

      svmModel
    }
  }

  implicit val logRegDataFormat = jsonFormat2(LogisticRegressionData)
  implicit val classficationWithSGDTrainFormat = jsonFormat10(ClassificationWithSGDTrainArgs)
  implicit val classificationWithSGDPredictFormat = jsonFormat3(ClassificationWithSGDPredictArgs)
  implicit val classificationWithSGDTestFormat = jsonFormat4(ClassificationWithSGDTestArgs)
  implicit val svmDataFormat = jsonFormat2(SVMData)
  implicit val kmeansDataFormat = jsonFormat3(KMeansData)
  implicit val kmeansModelTrainReturnFormat = jsonFormat2(KMeansTrainReturn)
  implicit val kmeansModelLoadFormat = jsonFormat8(KMeansTrainArgs)
  implicit val kmeansModelPredictFormat = jsonFormat3(KMeansPredictArgs)
  implicit val libSvmDataFormat = jsonFormat2(LibSvmData)
  implicit val libSvmModelFormat = jsonFormat19(LibSvmTrainArgs)
  implicit val libSvmPredictFormat = jsonFormat3(LibSvmPredictArgs)
  implicit val libSvmScoreFormat = jsonFormat2(LibSvmScoreArgs)
  implicit val libSvmScoreReturnFormat = jsonFormat1(LibSvmScoreReturn)
  implicit val libSvmTestFormat = jsonFormat4(LibSvmTestArgs)

}
