package com.intel.intelanalytics.domain.model

import com.intel.intelanalytics.domain.{ HasId, IAUri }
import org.joda.time.DateTime
import spray.json.{ JsValue, JsObject }

case class Model(id: Long,
                 name: String,
                 modelType: String,
                 description: Option[String],
                 statusId: Long,
                 data: Option[JsObject] = None,
                 createdOn: DateTime,
                 modifiedOn: DateTime,
                 createdByUserId: Option[Long] = None,
                 modifiedByUserId: Option[Long] = None) extends HasId with IAUri {
  require(id >= 0, "id must be zero or greater")
  require(name != null, "name must not be null")
  require(modelType != null, "modelType must not be null")
  require(name.trim.length > 0, "name must not be empty or whitespace")
  def entity = "model"

  def isLogisticRegressionModel: Boolean = {
    modelType.equals("LogisticRegression")
  }

}
