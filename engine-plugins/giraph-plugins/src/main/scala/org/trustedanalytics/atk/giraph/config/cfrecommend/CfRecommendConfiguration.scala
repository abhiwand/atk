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

package org.trustedanalytics.atk.giraph.config.cfrecommend

import org.trustedanalytics.atk.domain.schema.Schema
import org.apache.commons.lang3.StringUtils

import CfRecommendJsonFormat._

/**
 * Config for CfRecommend Input
 * @param userParquetFileLocation parquet input frame
 */
case class CfRecommendInputFormatConfig(userParquetFileLocation: String,
                                        itemParquetFileLocation: String,
                                        frameSchema: Schema) {
  require(StringUtils.isNotBlank(userParquetFileLocation), "user file location is required")
  require(StringUtils.isNotBlank(itemParquetFileLocation), "item file location is required")
  require(frameSchema != null, "input frame schema is required")
}

/**
 * Configuration for CfRecommend Output
 * @param recommendResultsFileLocation parquet output frame file location in HDFS
 */
case class CfRecommendOutputFormatConfig(recommendResultsFileLocation: String) {
  require(StringUtils.isNotBlank(recommendResultsFileLocation), "cf recommend results file location is required")
}
