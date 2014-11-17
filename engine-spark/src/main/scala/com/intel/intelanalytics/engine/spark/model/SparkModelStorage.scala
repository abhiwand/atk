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
import com.intel.intelanalytics.NotFoundException
import com.intel.intelanalytics.domain.model.{ ModelLoad, Model, ModelTemplate }
import com.intel.intelanalytics.engine.ModelStorage
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.plugin.SparkInvocation
import com.intel.intelanalytics.repository.MetaStore
import com.intel.intelanalytics.security.UserPrincipal
import spray.json.{ JsValue, JsObject }

/**
 * Front end for Spark to create and manage models.
 * @param metaStore Repository for model meta data.
 */

class SparkModelStorage(metaStore: MetaStore) extends ModelStorage with EventLogging {

  /** Lookup a Model, Throw an Exception if not found */
  override def expectModel(modelId: Long): Model = {
    lookup(modelId).getOrElse(throw new NotFoundException("model", modelId.toString))
  }

  /**
   * Deletes a model from the metastore.
   * @param model Model metadata object.
   */
  override def drop(model: Model): Unit = {
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
   * @param model The model being registered.
   * @param user The user creating the model.
   * @return Model metadata.
   */
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

  /**
   * Renames a model in the metastore.
   * @param model The model being renamed
   * @param newName The name the model is being renamed to.
   * @return Model metadata
   */
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

  /**
   * Obtain the model metadata for a range of model IDs.
   * @param user The user listing the model.
   * @return Sequence of model metadata objects.
   */
  override def getModels()(implicit user: UserPrincipal): Seq[Model] = {
    metaStore.withSession("spark.modelstorage.getModels") {
      implicit session =>
        {
          metaStore.modelRepo.scanAll()
        }
    }
  }

  /**
   * Store the result of running the train data on a model
   * @param model The model to update
   * @param newData JsObject storing the result of training.
   */

  override def updateModel(model: Model, newData: JsObject)(implicit user: UserPrincipal): Model = {
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