package org.apache.spark.mllib.classification.mllib.plugins

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.classification.LogisticRegressionModel
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

object LogisticRegressionJsonProtocol {

  implicit object SparseVectorFormat extends JsonFormat[SparseVector] {
    override def write(obj: SparseVector): JsValue = {

      JsObject(
        "size" -> JsNumber(obj.size),
        "indices" -> new JsArray(obj.indices.map(i => JsNumber(i)).toList),
        "values" -> new JsArray(obj.values.map(d => JsNumber(d)).toList)
      )
    }

    override def read(json: JsValue): SparseVector = {
      val fields = json.asJsObject.fields
      val size = fields.get("size").get.asInstanceOf[JsNumber].value.intValue
      val indices = fields.get("indices").get.asInstanceOf[JsArray].elements.map(i=>i.asInstanceOf[JsNumber].value.intValue).toArray
      val values = fields.get("values").get.asInstanceOf[JsArray].elements.map(i=>i.asInstanceOf[JsNumber].value.doubleValue).toArray

      new SparseVector(size, indices, values)
    }
  }

  implicit object DenseVectorFormat extends JsonFormat[DenseVector] {
    override def write(obj: DenseVector): JsValue = {
      JsObject(
        "values" -> new JsArray(obj.values.map(d => JsNumber(d)).toList)
      )
    }

    override def read(json: JsValue): DenseVector = {
      val fields = json.asJsObject.fields

      val values = fields.get("values").get.asInstanceOf[JsArray].elements.map(i=>i.asInstanceOf[JsNumber].value.doubleValue).toArray
      new DenseVector(values)
    }
  }

  implicit object LogisticRegressionModelFormat extends JsonFormat[LogisticRegressionModel] {
    override def write(obj: LogisticRegressionModel): JsValue = {
      val weights = obj.weights match {
        case sv: SparseVector => SparseVectorFormat.write(sv)
        case dv: DenseVector => DenseVectorFormat.write(dv)
        case _ => throw new IllegalArgumentException("Weights do not conform to Sparse or Dense Vector formats.")
      }
      JsObject(
        "weights" -> weights,
        "intercept" -> JsNumber(obj.intercept)
      )
    }

    override def read(json: JsValue): LogisticRegressionModel = {
      val fields = json.asJsObject.fields
      val intercept = fields.get("intercept").getOrElse(throw new IllegalArgumentException("Error in de-serialization: Missing intercept.")).asInstanceOf[JsNumber].value.doubleValue()

      val weights = fields.get("weights").map(v => {
        if (v.asJsObject.fields.get("size").isDefined) {
          SparseVectorFormat.read(v)
        }
        else {
          DenseVectorFormat.read(v)
        }
      }).get

      new LogisticRegressionModel(weights, intercept)
    }

  }

}

