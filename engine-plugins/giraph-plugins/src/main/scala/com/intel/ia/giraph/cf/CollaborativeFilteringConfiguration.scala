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

package com.intel.ia.giraph.cf

import com.intel.intelanalytics.domain.schema.Schema
import org.apache.commons.lang3.StringUtils
import org.apache.giraph.conf.GiraphConfiguration
import org.apache.hadoop.conf.Configuration
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Config for Input
 * @param parquetFileLocation parquet input frame
 */
case class CollaborativeFilteringInputFormatConfig(parquetFileLocation: String,
                                                   frameSchema: Schema) {
  require(StringUtils.isNotBlank(parquetFileLocation), "input file location is required")
  require(frameSchema != null, "input frame schema is required")
}

/**
 * Configuration for Output
 * @param userFileLocation parquet output frame file location in HDFS
 */
case class CollaborativeFilteringOutputFormatConfig(userFileLocation: String, itemFileLocation: String) {
  require(StringUtils.isNotBlank(userFileLocation), "user output file is required")
  require(StringUtils.isNotBlank(userFileLocation), "item output file is required")
}

/**
 *
 * @param inputFormatConfig input format configuration
 * @param outputFormatConfig output format configuration
 * @param userColName db user column name
 * @param itemColName db item column name
 * @param ratingColName db rating column name
 * @param evaluationFunction "alg" or "cgd"
 * @param numFactors factors for the output array
 * @param maxIterations mac iterations
 * @param convergenceThreshold convergence threshold
 * @param lambda lambda
 * @param biasOn alg bias
 * @param maxValue max value
 * @param minValue min value
 * @param learningCurveInterval learning curve interval
 */
case class CollaborativeFilteringConfig(inputFormatConfig: CollaborativeFilteringInputFormatConfig,
                                        outputFormatConfig: CollaborativeFilteringOutputFormatConfig,
                                        userColName: String,
                                        itemColName: String,
                                        ratingColName: String,
                                        evaluationFunction: String,
                                        numFactors: Int,
                                        maxIterations: Int,
                                        convergenceThreshold: Double,
                                        lambda: Float,
                                        biasOn: Boolean,
                                        maxValue: Float,
                                        minValue: Float,
                                        learningCurveInterval: Int,
                                        cgdIterations: Int) {

  def this(inputFormatConfig: CollaborativeFilteringInputFormatConfig,
           outputFormatConfig: CollaborativeFilteringOutputFormatConfig,
           args: CollaborativeFilteringArgs) = {
    this(inputFormatConfig,
      outputFormatConfig,
      args.userColName,
      args.itemColName,
      args.ratingColName,
      args.getEvaluationFunction,
      args.getNumFactors,
      args.getMaxIterations,
      args.getConvergenceThreshold,
      args.getLambda,
      args.getBias,
      args.getMinValue,
      args.getMaxValue,
      args.getLearningCurveInterval,
      args.getCgdIterations)
  }
  require(inputFormatConfig != null, "input format is required")
  require(outputFormatConfig != null, "output format is required")
}

/**
 * JSON formats.
 */
object CollaborativeFilteringConfigJSONFormat {
  implicit val inputFormatConfigFormat = jsonFormat2(CollaborativeFilteringInputFormatConfig)
  implicit val outputFormatConfigFormat = jsonFormat2(CollaborativeFilteringOutputFormatConfig)
  implicit val configFormat = jsonFormat15(CollaborativeFilteringConfig)
}

import CollaborativeFilteringConfigJSONFormat._

/**
 * Wrapper so that we can use simpler API for getting configuration settings.
 *
 * All of the settings can go into one JSON string so we don't need a bunch of String
 * constants passed around.
 */
class CollaborativeFilteringConfiguration(other: Configuration) extends GiraphConfiguration(other) {

  private val ConfigPropertyName = "collaborativefiltering.config"

  def this() = {
    this(new Configuration)
  }

  /** make sure required properties are set */
  def validate(): Unit = {
    require(get(ConfigPropertyName) != null, "labelPropagation.config property was not set in the Configuration")
  }

  def getConfig: CollaborativeFilteringConfig = {
    JsonParser(get(ConfigPropertyName)).asJsObject.convertTo[CollaborativeFilteringConfig]
  }

  def setConfig(value: CollaborativeFilteringConfig): Unit = {
    set(ConfigPropertyName, value.toJson.compactPrint)
  }
}
