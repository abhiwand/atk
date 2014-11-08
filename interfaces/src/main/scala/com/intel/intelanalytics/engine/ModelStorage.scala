package com.intel.intelanalytics.engine

import com.intel.intelanalytics.domain.model.{ ModelLoad, ModelTemplate, Model }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.security.UserPrincipal
import spray.json.{ JsValue, JsObject }

trait ModelStorage {

  def expectModel(modelId: Long): Model

  def lookup(id: Long): Option[Model]

  def createModel(model: ModelTemplate)(implicit user: UserPrincipal): Model

  def renameModel(model: Model, newName: String): Model

  def drop(model: Model)

  def getModels()(implicit user: UserPrincipal): Seq[Model]

  def getModelByName(name: String)(implicit user: UserPrincipal): Option[Model]

  def loadModel(modelLoad: ModelLoad, invocation: Invocation)(implicit user: UserPrincipal): Model

  def updateModel(model: Model, newData: JsObject)(implicit user: UserPrincipal): Model

}
