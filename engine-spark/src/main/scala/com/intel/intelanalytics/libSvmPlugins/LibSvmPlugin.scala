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

package com.intel.intelanalytics.libSvmPlugins

import com.intel.intelanalytics.domain.CreateEntityArgs
import com.intel.intelanalytics.domain.model.{ GenericNewModelArgs, ModelEntity }
import com.intel.intelanalytics.engine.plugin.{ ApiMaturityTag, Invocation }
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.mllib.ia.plugins.MLLibJsonProtocol._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

class LibSvmPlugin extends SparkCommandPlugin[GenericNewModelArgs, ModelEntity] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:libsvm/new"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  override def execute(arguments: GenericNewModelArgs)(implicit invocation: Invocation): ModelEntity =
    {
      val models = engine.models
      models.createModel(CreateEntityArgs(name = arguments.name, entityType = Some("model:libsvm")))
    }
}

