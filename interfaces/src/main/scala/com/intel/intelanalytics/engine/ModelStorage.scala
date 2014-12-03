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

package com.intel.intelanalytics.engine

import com.intel.intelanalytics.domain.model.{ ModelLoad, ModelTemplate, Model }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.security.UserPrincipal
import spray.json.{ JsValue, JsObject }

trait ModelStorage {

  def expectModel(modelId: Long): Model

  def lookup(id: Long): Option[Model]

  def createModel(model: ModelTemplate)(implicit invocation: Invocation): Model

  def renameModel(model: Model, newName: String): Model

  def drop(model: Model)

  def getModels()(implicit invocation: Invocation): Seq[Model]

  def getModelByName(name: String)(implicit invocation: Invocation): Option[Model]

  def updateModel(model: Model, newData: JsObject)(implicit invocation: Invocation): Model

}
