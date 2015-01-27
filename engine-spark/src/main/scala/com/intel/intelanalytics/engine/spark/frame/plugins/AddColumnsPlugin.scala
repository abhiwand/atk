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

package com.intel.intelanalytics.engine.spark.frame.plugins

import com.intel.intelanalytics.domain.UriReference
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.domain.schema.{ FrameSchema, Column }
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.{ FrameRDD, PythonRDDStorage, SparkFrameData }
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.sql

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Adds one or more new columns to the frame by evaluating the given func on each row.
 */
class AddColumnsPlugin extends SparkCommandPlugin[AddColumnsArgs, FrameEntity] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/add_columns"

  /**
   * Adds one or more new columns to the frame by evaluating the given func on each row.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: AddColumnsArgs)(implicit invocation: Invocation): FrameEntity = {
    val frame: SparkFrameData = resolve(arguments.frame)
    val newColumns = arguments.columnNames.zip(arguments.columnTypes.map(x => x: DataType))
    //    val newSchema = frame.meta.schema.addColumns(newColumns.map { case (name, dataType) => Column(name, dataType) })
    val newSchema = new FrameSchema(newColumns.map { case (name, dataType) => Column(name, dataType) })

    val finalSchema = frame.meta.schema.addColumns(newColumns.map { case (name, dataType) => Column(name, dataType) })

    /* TODO: In Memory Caching is ineffective for certain large datasets, we should be experimenting with MEM_SER or MEM_DISK_SER persistance
       TODO: With cache the performance is better for datasets which can be cached, but the performance drops due to RDD
       TODO: recomputation when we reach the limit. Additional experimentation required.
    */
    // Update the data
    val rdd = PythonRDDStorage.mapWith(frame.data.cache(), arguments.udf, newSchema, sc)
    save(new SparkFrameData(frame.meta.withSchema(finalSchema), frame.data.zipFrameRDD(rdd))).meta
  }
}
