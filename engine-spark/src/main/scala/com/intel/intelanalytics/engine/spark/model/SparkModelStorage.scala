//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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

package com.intel.intelanalytics.engine.spark.model

import com.intel.event.EventLogging
import com.intel.intelanalytics.{ DuplicateNameException, EventLoggingImplicits, NotFoundException }
import com.intel.intelanalytics.domain.model._
import com.intel.intelanalytics.domain.{ Status, CreateEntityArgs, EntityManager }
import com.intel.intelanalytics.engine.{ EntityTypeRegistry, ModelStorage }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.plugin.SparkInvocation
import com.intel.intelanalytics.repository.{ SlickMetaStoreComponent, MetaStore }
import com.intel.intelanalytics.security.UserPrincipal
import spray.json.{ JsValue, JsObject }
import scala.Some
import org.apache.spark.SparkContext
import com.intel.intelanalytics.engine.spark.SparkAutoPartitioner
import com.intel.intelanalytics.component.ClassLoaderAware
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

    override def getMetaData(reference: Reference)(implicit invocation: Invocation): MetaData = new ModelMeta(expectModel(reference.id))

    override def create(args: CreateEntityArgs)(implicit invocation: Invocation): Reference =
      storage.createModel(args)

    override def getReference(id: Long)(implicit invocation: Invocation): Reference = ModelReference(id)

    implicit def modelToRef(model: ModelEntity)(implicit invocation: Invocation): Reference = ModelReference(model.id)

    implicit def sc(implicit invocation: Invocation): SparkContext = invocation.asInstanceOf[SparkInvocation].sparkContext

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
      drop(meta.meta)
    }
  }

  EntityTypeRegistry.register(ModelEntityType, SparkModelManagement)

  /** Lookup a Model, Throw an Exception if not found */
  override def expectModel(modelId: Long): ModelEntity = {
    lookup(modelId).getOrElse(throw new NotFoundException("model", modelId.toString))
  }

  /**
   * Deletes a model from the metastore.
   * @param model Model metadata object.
   */
  override def drop(model: ModelEntity): Unit = {
    metaStore.withSession("spark.modelstorage.drop") {
      implicit session =>
        {
          metaStore.modelRepo.delete(model.id)
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
   * @param model The model being renamed
   * @param newName The name the model is being renamed to.
   * @return Model metadata
   */
  override def renameModel(model: ModelEntity, newName: String): ModelEntity = {
    metaStore.withSession("spark.modelstorage.rename") {
      implicit session =>
        {
          val check = metaStore.modelRepo.lookupByName(Some(newName))
          if (check.isDefined) {
            throw new RuntimeException("Model with same name exists. Rename aborted.")
          }

          val newModel = model.copy(name = Some(newName))
          metaStore.modelRepo.update(newModel).get
        }
    }
  }

  /**
   * Get the metadata for a model from its unique ID.
   * @param id ID being looked up.
   * @return Future of Model metadata.
   */
  override def lookup(id: Long): Option[ModelEntity] = {
    metaStore.withSession("spark.modelstorage.lookup") {
      implicit session =>
        {
          metaStore.modelRepo.lookup(id)
        }
    }
  }

  override def getModelByName(name: Option[String])(implicit invocation: Invocation): Option[ModelEntity] = {
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
          metaStore.modelRepo.scanAll().filter(m => m.statusId != Status.Deleted && m.statusId != Status.Dead && m.name.isDefined)
        }
    }
  }

  /**
   * Store the result of running the train data on a model
   * @param model The model to update
   * @param newData JsObject storing the result of training.
   */

  override def updateModel(model: ModelEntity, newData: JsObject)(implicit invocation: Invocation): ModelEntity = {
    metaStore.withSession("spark.modelstorage.updateModel") {
      implicit session =>
        {
          expectModel(model.id)
          val newModel = model.copy(data = Option(newData))
          metaStore.modelRepo.update(newModel).get
        }
    }
  }

}
