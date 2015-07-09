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

package com.intel.taproot.analytics.engine.spark.model

import com.intel.taproot.event.EventLogging
import com.intel.taproot.analytics.{ DuplicateNameException, EventLoggingImplicits, NotFoundException }
import com.intel.taproot.analytics.domain.model._
import com.intel.taproot.analytics.domain.{ Status, CreateEntityArgs, EntityManager }
import com.intel.taproot.analytics.engine.{ EntityTypeRegistry, ModelStorage }
import com.intel.taproot.analytics.engine.plugin.Invocation
import com.intel.taproot.analytics.engine.spark.plugin.SparkInvocation
import com.intel.taproot.analytics.repository.{ SlickMetaStoreComponent, MetaStore }
import com.intel.taproot.analytics.security.UserPrincipal
import spray.json.{ JsValue, JsObject }
import scala.Some
import org.apache.spark.SparkContext
import com.intel.taproot.analytics.component.ClassLoaderAware
import scala.slick.model

import scala.util.Try

/**
 * Front end for Spark to create and manage models.
 * @param metaStore Repository for model meta data.
 */

class SparkModelStorage(metaStore: MetaStore)
    extends ModelStorage with EventLogging with EventLoggingImplicits with ClassLoaderAware {
  storage =>
  def updateLastReadDate(model: ModelEntity): Try[ModelEntity] = {
    metaStore.withSession("model.updateLastReadDate") {
      implicit session =>
        metaStore.modelRepo.updateLastReadDate(model)
    }
  }

  object SparkModelManagement extends EntityManager[ModelEntityType.type] {

    override implicit val referenceTag = ModelEntityType.referenceTag

    override type Reference = ModelReference

    override type MetaData = ModelMeta

    override type Data = SparkModel

    override def getData(reference: Reference)(implicit invocation: Invocation): Data = {
      val meta = getMetaData(reference)
      new SparkModel(meta.meta)
    }

    override def getMetaData(reference: Reference)(implicit invocation: Invocation): MetaData = new ModelMeta(expectModel(reference))

    override def create(args: CreateEntityArgs)(implicit invocation: Invocation): Reference = storage.createModel(args)

    override def getReference(id: Long)(implicit invocation: Invocation): Reference = ModelReference(id)

    implicit def modelToRef(model: ModelEntity)(implicit invocation: Invocation): Reference = model.toReference

    /**
     * Save data of the given type, possibly creating a new object.
     */
    override def saveData(data: Data)(implicit invocation: Invocation): Data = {
      data // no data to save, this is a nop
    }

    /**
     * Creates an (empty) instance of the given type, reserving a URI
     */
    override def delete(reference: SparkModelStorage.this.SparkModelManagement.Reference)(implicit invocation: Invocation): Unit = {
      val meta = getMetaData(reference)
      drop(meta.meta.toReference)
    }
  }

  EntityTypeRegistry.register(ModelEntityType, SparkModelManagement)

  /** Lookup a Model, Throw an Exception if not found */
  override def expectModel(modelRef: ModelReference): ModelEntity = {
    metaStore.withSession("spark.modelstorage.lookup") {
      implicit session =>
        {
          metaStore.modelRepo.lookup(modelRef.id)
        }
    }.getOrElse(throw new NotFoundException("model", modelRef.toString))
  }

  /**
   * Deletes a model from the metastore.
   * @param modelRef Model metadata object.
   */
  override def drop(modelRef: ModelReference): Unit = {
    metaStore.withSession("spark.modelstorage.drop") {
      implicit session =>
        {
          metaStore.modelRepo.delete(modelRef.id)
          Unit
        }
    }
  }

  /**
   * Registers a new model.
   * @param createArgs arguments to create the model entity
   * @return Model metadata.
   */
  override def createModel(createArgs: CreateEntityArgs)(implicit invocation: Invocation): ModelEntity = {
    metaStore.withSession("spark.modelstorage.create") {
      implicit session =>
        {
          if (createArgs.name.isDefined) {
            metaStore.modelRepo.lookupByName(createArgs.name).foreach {
              existingModel =>
                throw new DuplicateNameException("model", createArgs.name.get, "Model with same name exists. Create aborted.")
            }
          }
          val modelTemplate = ModelTemplate(createArgs.name, createArgs.entityType.get)
          metaStore.modelRepo.insert(modelTemplate).get
        }
    }
  }

  /**
   * Renames a model in the metastore.
   * @param modelRef The model being renamed
   * @param newName The name the model is being renamed to.
   * @return Model metadata
   */
  override def renameModel(modelRef: ModelReference, newName: String): ModelEntity = {
    metaStore.withSession("spark.modelstorage.rename") {
      implicit session =>
        {
          val check = metaStore.modelRepo.lookupByName(Some(newName))
          if (check.isDefined) {
            throw new RuntimeException("Model with same name exists. Rename aborted.")
          }

          val newModel = expectModel(modelRef).copy(name = Some(newName))
          val renamed = metaStore.modelRepo.update(newModel).get
          metaStore.modelRepo.updateLastReadDate(renamed).get
        }
    }
  }

  override def getModelByName(name: Option[String]): Option[ModelEntity] = {
    metaStore.withSession("spark.modelstorage.getModelByName") {
      implicit session =>
        {
          metaStore.modelRepo.lookupByName(name)
        }
    }
  }

  /**
   * Obtain the model metadata for a range of model IDs.
   * @return Sequence of model metadata objects.
   */
  override def getModels()(implicit invocation: Invocation): Seq[ModelEntity] = {
    metaStore.withSession("spark.modelstorage.getModels") {
      implicit session =>
        {
          metaStore.modelRepo.scanAll().filter(m => m.statusId != Status.Deleted && m.statusId != Status.Deleted_Final && m.name.isDefined)
        }
    }
  }

  /**
   * Store the result of running the train data on a model
   * @param modelRef The model to update
   * @param newData JsObject storing the result of training.
   */

  override def updateModel(modelRef: ModelReference, newData: JsObject)(implicit invocation: Invocation): ModelEntity = {
    metaStore.withSession("spark.modelstorage.updateModel") {
      implicit session =>
        {
          val currentModel = expectModel(modelRef)
          val newModel = currentModel.copy(data = Option(newData))

          metaStore.modelRepo.update(newModel).get
        }
    }
  }

  /**
   * Set a model to be deleted on the next execution of garbage collection
   * @param model model to delete
   * @param invocation current invocation
   */
  override def scheduleDeletion(model: ModelEntity)(implicit invocation: Invocation): Unit = {
    metaStore.withSession("spark.modelstorage.scheduleDeletion") {
      implicit session =>
        {
          info(s"marking as ready to delete: model id:${model.id}, name:${model.name}")
          metaStore.modelRepo.updateReadyToDelete(model)
        }
    }
  }
}