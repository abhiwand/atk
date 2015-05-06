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

package org.apache.spark.sql.parquet.ia.giraph.frame

import com.intel.giraph.io.{ VertexData4LPWritable, LabelPropagationVertexId }
import com.intel.ia.giraph.lp.LabelPropagationConfiguration
import org.apache.giraph.io.VertexOutputFormat
import org.apache.hadoop.mapreduce._
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.parquet.RowWriteSupport
import parquet.hadoop.ParquetOutputFormat

/**
 * OutputFormat for parquet frame
 */
class LabelPropagationVertexOutputFormat extends VertexOutputFormat[LabelPropagationVertexId, VertexData4LPWritable, Nothing] {

  private val resultsOutputFormat = new ParquetOutputFormat[Row](new RowWriteSupport)

  override def createVertexWriter(context: TaskAttemptContext): LabelPropagationVertexWriter = {
    new LabelPropagationVertexWriter(new LabelPropagationConfiguration(context.getConfiguration), resultsOutputFormat)
  }

  override def checkOutputSpecs(context: JobContext): Unit = {
    new LabelPropagationConfiguration(context.getConfiguration).validate()
  }

  override def getOutputCommitter(context: TaskAttemptContext): OutputCommitter = {
    val outputFormatConfig = new LabelPropagationConfiguration(context.getConfiguration).labelPropagationConfig.outputFormatConfig

    // configure outputdir for committer
    context.getConfiguration.set("mapreduce.output.fileoutputformat.outputdir", outputFormatConfig.parquetFileLocation)
    val docCommitter = resultsOutputFormat.getOutputCommitter(context)

    new MultiOutputCommitter(List(docCommitter))
  }
}

