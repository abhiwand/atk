package com.intel.intelanalytics.engine.spark.model

import com.intel.event.EventLogging
import com.intel.intelanalytics.NotFoundException
import com.intel.intelanalytics.domain.model.{  ModelLoad, Model, ModelTemplate }
import com.intel.intelanalytics.engine.ModelStorage
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.plugin.SparkInvocation
import com.intel.intelanalytics.repository.MetaStore
import com.intel.intelanalytics.security.UserPrincipal
import spray.json.{ JsValue, JsObject }

class SparkModelStorage(metaStore: MetaStore) extends ModelStorage with EventLogging {

  override def expectModel(modelId: Long): Model = {
    lookup(modelId).getOrElse(throw new NotFoundException("model", modelId.toString))
  }

  override def drop(model: Model): Unit = {
    metaStore.withSession("spark.modelstorage.drop") {
      implicit session =>
        {
          metaStore.modelRepo.delete(model.id)
          Unit
        }
    }
  }

  override def createModel(model: ModelTemplate)(implicit user: UserPrincipal): Model = {
    metaStore.withSession("spark.modelstorage.create") {
      implicit session =>
        {
          val check = metaStore.modelRepo.lookupByName(model.name)
          if (check.isDefined) {
            throw new RuntimeException("Model with same name exists. Create aborted.")
          }
          metaStore.modelRepo.insert(model).get
        }
    }
  }

  override def renameModel(model: Model, newName: String): Model = {
    metaStore.withSession("spark.modelstorage.rename") {
      implicit session =>
        {
          val check = metaStore.modelRepo.lookupByName(newName)
          if (check.isDefined) {
            throw new RuntimeException("Model with same name exists. Rename aborted.")
          }

          val newModel = model.copy(name = newName)
          metaStore.modelRepo.update(newModel).get
        }
    }
  }

  /**
   * Get the metadata for a model from its unique ID.
   * @param id ID being looked up.
   * @return Future of Model metadata.
   */
  override def lookup(id: Long): Option[Model] = {
    metaStore.withSession("spark.modelstorage.lookup") {
      implicit session =>
        {
          metaStore.modelRepo.lookup(id)
        }
    }
  }

  override def getModelByName(name: String)(implicit user: UserPrincipal): Option[Model] = {
    metaStore.withSession("spark.modelstorage.getModelByName") {
      implicit session =>
        {
          metaStore.modelRepo.lookupByName(name)
        }
    }
  }

  override def getModels()(implicit user: UserPrincipal): Seq[Model] = {
    metaStore.withSession("spark.modelstorage.getModels") {
      implicit session =>
        {
          metaStore.modelRepo.scanAll()
        }
    }
  }

  /**
   * Loads new data in an existing model in the model database.
   * @param modelLoad Command arguments.
   * @param user The user loading the model.
   * @return
   */
  override def loadModel(modelLoad: ModelLoad, invocation: Invocation)(implicit user: UserPrincipal): Model = {
    withContext("se.loadmodel") {
      metaStore.withSession("spark.modelstorage.load") {
        implicit session =>
          {
            val sparkContext = invocation.asInstanceOf[SparkInvocation].sparkContext

            val model = lookup(modelLoad.model.id).get

            model
          }
      }
    }
  }

  override def updateModel(model: Model, newData: JsObject)(implicit user: UserPrincipal): Model = {
    metaStore.withSession("spark.modelstorage.updateModel") {
      implicit session =>
        {
          val check = metaStore.modelRepo.lookupByName(model.name)
          if (!check.isDefined) {
            throw new RuntimeException("Model with this name does not exist. Update aborted.")
          }
          val newModel = model.copy(data = Option(newData))
          metaStore.modelRepo.update(newModel).get
        }
    }
  }

}
