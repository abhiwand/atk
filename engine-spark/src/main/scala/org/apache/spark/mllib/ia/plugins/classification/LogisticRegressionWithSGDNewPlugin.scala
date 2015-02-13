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
package org.apache.spark.mllib.ia.plugins.classification

import com.intel.intelanalytics.domain.{ CreateEntityArgs, Naming }
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.{ FrameEntity, FrameMeta }
import com.intel.intelanalytics.domain.model.{ GenericNewModelArgs, ModelEntity, ClassificationWithSGDPredictArgs }
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.engine.Rows.Row
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.{ SparkFrameData }
import org.apache.spark.frame.FrameRDD
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.ia.plugins.MLLibJsonProtocol
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.catalyst.expressions.GenericRow
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import org.apache.spark.mllib.ia.plugins.MLLibJsonProtocol._

/**
 * Create a 'new' instance of this model
 */
class LogisticRegressionWithSGDNewPlugin extends SparkCommandPlugin[GenericNewModelArgs, ModelEntity] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:logistic_regression/new"

  override def execute(arguments: GenericNewModelArgs)(implicit invocation: Invocation): ModelEntity =
    {
      val models = engine.models
      models.createModel(CreateEntityArgs(name = arguments.name, entityType = Some("model:logistic_regression")))
    }
}
