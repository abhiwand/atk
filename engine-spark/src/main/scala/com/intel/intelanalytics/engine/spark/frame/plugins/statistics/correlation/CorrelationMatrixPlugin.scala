
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

package com.intel.intelanalytics.engine.spark.frame.plugins.statistics.correlation

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.DataFrame
import com.intel.intelanalytics.domain.frame.FrameMeta
import com.intel.intelanalytics.domain.frame.{ FrameMeta, CorrelationMatrixArguments, DataFrame, DataFrameTemplate }
import com.intel.intelanalytics.domain.Naming
import com.intel.intelanalytics.domain.schema.Column
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.domain.schema.FrameSchema
import com.intel.intelanalytics.domain.schema.{ Column, FrameSchema, DataTypes, Schema }
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.FrameRDD
import com.intel.intelanalytics.engine.spark.frame.SparkFrameData
import com.intel.intelanalytics.engine.spark.frame.{ SparkFrameData, FrameRDD }
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.rdd.RDD

import scala.concurrent.ExecutionContext

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Calculate correlation matrix for the specified columns
 */
class CorrelationMatrixPlugin extends SparkCommandPlugin[CorrelationMatrixArguments, DataFrame] {

  /**
   * The name of the command
   */
  override def name: String = "frame/correlation_matrix"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = Some(CommandDoc(oneLineSummary = "Calculate correlation matrix for two or more columns",
    extendedSummary = Some("""

        Compute the correlation matrix for two or more columns.

        Parameters
        ----------
        columns: [ str | list of str ]
            The names of the column from which to compute the matrix

        Returns
        -------
        A matrix with the correlation values for the columns

        Notes
        -----
        This function applies only to columns containing numerical data.

        Examples
        --------
        Consider Frame *my_frame*, which accesses a frame that contains a single column named *obs*::

            cor_matirx = my_frame.correlation_matrix(['col_0', 'col_1', 'col_2'])

            cor_matrix.inspect()

                           """)))
  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: CorrelationMatrixArguments)(implicit invocation: Invocation) = 7

  /**
   * Calculate correlation matrix for the specified columns
   *
   * @param invocation information about the user and the circumstances at the time of the call, as well as a function
   *                   that can be called to produce a SparkContext that can be used during this invocation
   * @param arguments input specification for correlation matrix
   * @return value of type declared as the Return type
   */
  override def execute(arguments: CorrelationMatrixArguments)(implicit invocation: Invocation): DataFrame = {

    val frame: SparkFrameData = resolve(arguments.frame)
    frame.meta.schema.validateColumnsExist(arguments.dataColumnNames)
    // load frame as RDD
    val rdd = frame.data

    val inputDataColumnNamesAndTypes: List[Column] = arguments.dataColumnNames.map({ name => Column(name, DataTypes.float64) }).toList
    val correlationRDD = Correlation.correlationMatrix(rdd, arguments.dataColumnNames)

    val schema = FrameSchema(inputDataColumnNamesAndTypes)
    tryNew(arguments.matrixName) { newFrame: FrameMeta =>
      save(new SparkFrameData(newFrame.meta, new FrameRDD(schema, correlationRDD)))
    }.meta
  }
}
