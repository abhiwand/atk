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

package com.intel.ia.giraph.lp

import com.intel.intelanalytics.domain.frame.FrameReference
import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.domain.model.ModelReference
import com.intel.intelanalytics.domain.schema.Schema
import org.apache.commons.lang3.StringUtils
import org.apache.giraph.conf.GiraphConfiguration
import org.apache.hadoop.conf.Configuration
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import LabelPropagationJsonFormat._

/**
 * Config for Input
 * @param parquetFileLocation parquet input frame
 */
case class LabelPropagationInputFormatConfig(parquetFileLocation: String,
                                             frameSchema: Schema) {
  require(StringUtils.isNotBlank(parquetFileLocation), "input file location is required")
  require(frameSchema != null, "input frame schema is required")
}

/**
 * Configuration for Output
 * @param parquetFileLocation parquet output frame file location in HDFS
 */
case class LabelPropagationOutputFormatConfig(parquetFileLocation: String) {
  require(StringUtils.isNotBlank(parquetFileLocation), "output file location is required")
}

/**
 *
 * @param inputFormatConfig
 * @param outputFormatConfig
 * @param model
 * @param frame
 * @param sourceIdColumnName
 * @param destinationIdColumnName
 * @param edgeWeightColumnName
 * @param sourceIdLabelColumnName
 * @param destinationIdLabelColumnName
 * @param vectorValue
 * @param maxSupersteps
 * @param convergenceThreshold
 * @param anchorThreshold
 * @param lpLambda
 * @param validateGraphStructure
 */
case class LabelPropagationConfig(inputFormatConfig: LabelPropagationInputFormatConfig,
                                  outputFormatConfig: LabelPropagationOutputFormatConfig,
                                  model: ModelReference,
                                  frame: FrameReference,
                                  sourceIdColumnName: String,
                                  destinationIdColumnName: String,
                                  edgeWeightColumnName: String,
                                  sourceIdLabelColumnName: String,
                                  destinationIdLabelColumnName: String,
                                  vectorValue: Boolean,
                                  maxSupersteps: Option[Int] = None,
                                  convergenceThreshold: Option[Double] = None,
                                  anchorThreshold: Option[Double] = None,
                                  lpLambda: Option[Double] = None,
                                  validateGraphStructure: Option[Boolean] = None) {

  def this(inputFormatConfig: LabelPropagationInputFormatConfig,
           outputFormatConfig: LabelPropagationOutputFormatConfig,
           args: LabelPropagationArgs) = {
    this(inputFormatConfig,
      outputFormatConfig,
      args.model,
      args.frame,
      args.sourceIdColumnName,
      args.destinationIdColumnName,
      args.edgeWeightColumnName,
      args.sourceIdLabelColumnName,
      args.destinationIdLabelColumnName,
      args.vectorValue,
      args.maxIterations)
  }
  require(inputFormatConfig != null, "input format is required")
  require(outputFormatConfig != null, "output format is required")
}

/**
 * JSON formats.
 */
object LabelPropagationConfigJSONFormat {
  implicit val inputFormatConfigFormat = jsonFormat2(LabelPropagationInputFormatConfig)
  implicit val outputFormatConfigFormat = jsonFormat1(LabelPropagationOutputFormatConfig)
  implicit val configFormat = jsonFormat15(LabelPropagationConfig)
}

import LabelPropagationConfigJSONFormat._

/**
 * Wrapper so that we can use simpler API for getting configuration settings.
 *
 * All of the settings can go into one JSON string so we don't need a bunch of String
 * constants passed around.
 */
class LabelPropagationConfiguration(other: Configuration) extends GiraphConfiguration(other) {

  private val ConfigPropertyName = "labelPropagation.config"

  def this() = {
    this(new Configuration)
  }

  /** make sure required properties are set */
  def validate(): Unit = {
    require(get(ConfigPropertyName) != null, "labelPropagation.config property was not set in the Configuration")
  }

  def labelPropagationConfig: LabelPropagationConfig = {
    JsonParser(get(ConfigPropertyName)).asJsObject.convertTo[LabelPropagationConfig]
  }

  def setConfig(value: LabelPropagationConfig): Unit = {
    set(ConfigPropertyName, value.toJson.compactPrint)
  }
}
