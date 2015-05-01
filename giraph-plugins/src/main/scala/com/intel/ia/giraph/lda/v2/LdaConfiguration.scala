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

package com.intel.ia.giraph.lda.v2

import com.intel.intelanalytics.domain.schema.Schema
import org.apache.commons.lang3.StringUtils
import org.apache.giraph.conf.GiraphConfiguration
import org.apache.hadoop.conf.Configuration
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import LdaJsonFormat._

/**
 * Config for LDA Input
 * @param parquetFileLocation parquet input frame
 */
case class LdaInputFormatConfig(parquetFileLocation: String,
                                frameSchema: Schema) {
  require(StringUtils.isNotBlank(parquetFileLocation), "input file location is required")
  require(frameSchema != null, "input frame schema is required")
}

/**
 * Configuration for LDA Output
 * @param documentResultsFileLocation parquet output frame file location in HDFS
 * @param wordResultsFileLocation parquet output frame file location in HDFS
 */
case class LdaOutputFormatConfig(documentResultsFileLocation: String, wordResultsFileLocation: String) {
  require(StringUtils.isNotBlank(documentResultsFileLocation), "document lda results file location is required")
  require(StringUtils.isNotBlank(wordResultsFileLocation), "word lda results file location is required")
}

/**
 * Configuration settings for Lda
 * @param inputFormatConfig where to read input from
 * @param outputFormatConfig where to write output to
 * @param documentColumnName column name that contains the "documents"
 * @param wordColumnName column name that contains the "words"
 * @param wordCountColumnName column name that contains "word count"
 * @param maxIterations see LdaTrainArgs for doc
 * @param alpha see LdaTrainArgs for doc
 * @param beta see LdaTrainArgs for doc
 * @param convergenceThreshold see LdaTrainArgs for doc
 * @param evaluationCost see LdaTrainArgs for doc
 * @param numTopics see LdaTrainArgs for doc
 */
case class LdaConfig(inputFormatConfig: LdaInputFormatConfig,
                     outputFormatConfig: LdaOutputFormatConfig,
                     documentColumnName: String,
                     wordColumnName: String,
                     wordCountColumnName: String,
                     maxIterations: Long,
                     alpha: Float,
                     beta: Float,
                     convergenceThreshold: Float,
                     evaluationCost: Boolean,
                     numTopics: Int) {

  def this(inputFormatConfig: LdaInputFormatConfig, outputFormatConfig: LdaOutputFormatConfig, args: LdaTrainArgs) = {
    this(inputFormatConfig,
      outputFormatConfig,
      args.documentColumnName,
      args.wordColumnName,
      args.wordCountColumnName,
      args.getMaxIterations,
      args.getAlpha,
      args.getBeta,
      args.getConvergenceThreshold,
      args.getEvaluateCost,
      args.getNumTopics)
  }
  require(inputFormatConfig != null, "input format is required")
  require(outputFormatConfig != null, "output format is required")
  require(StringUtils.isNotBlank(documentColumnName), "document column name is required")
  require(StringUtils.isNotBlank(wordColumnName), "word column name is required")
  require(StringUtils.isNotBlank(wordCountColumnName), "word count column name is required")
  require(maxIterations > 0, "Max iterations should be greater than 0")
  require(alpha > 0, "Alpha should be greater than 0")
  require(beta > 0, "Beta should be greater than 0")
  require(numTopics > 0, "Number of topics (K) should be greater than 0")
}

/**
 * JSON formats needed by Lda.
 */
object LDAConfigJSONFormat {
  implicit val ldaInputFormatConfigFormat = jsonFormat2(LdaInputFormatConfig)
  implicit val ldaOutputFormatConfigFormat = jsonFormat2(LdaOutputFormatConfig)
  implicit val ldaConfigFormat = jsonFormat11(LdaConfig)
}

import LDAConfigJSONFormat._

/**
 * Wrapper so that we can use simpler API for getting configuration settings.
 *
 * All of the settings can go into one JSON string so we don't need a bunch of String
 * constants passed around.
 */
class LdaConfiguration(other: Configuration) extends GiraphConfiguration(other) {

  private val LdaConfigPropertyName = "lda.config"

  def this() = {
    this(new Configuration)
  }

  /** make sure required properties are set */
  def validate(): Unit = {
    require(get(LdaConfigPropertyName) != null, "lda.config property was not set in the Configuration")
  }

  def ldaConfig: LdaConfig = {
    // all of the settings can go into one JSON string so we don't need a bunch of String constants passed around
    JsonParser(get(LdaConfigPropertyName)).asJsObject.convertTo[LdaConfig]
  }

  def setLdaConfig(value: LdaConfig): Unit = {
    // all of the settings can go into one JSON string so we don't need a bunch of String constants passed around
    set(LdaConfigPropertyName, value.toJson.compactPrint)
  }
}
