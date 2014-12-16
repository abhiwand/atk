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

package com.intel.intelanalytics.engine.spark.frame.plugins.exporthdfs

import java.nio.file.FileSystem

import com.intel.intelanalytics.domain.{BoolValue, DoubleValue, LongValue}
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.ExportCsvArguments
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.SparkFrameData
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine.Rows._

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Calculate covariance for the specified columns
 */
class ExportHDFSPlugin extends SparkCommandPlugin[ExportCsvArguments, BoolValue] {

  /**
   * The name of the command
   */
  override def name: String = "frame/export_hdfs"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = Some(CommandDoc(oneLineSummary = "Calculate covariance for exactly two columns",
    extendedSummary = Some("""

        Compute the covariance for two columns.

        Parameters
        ----------
        columns: [ str | list of str ]
            The names 2 columns from which to compute the covariance

        Returns
        -------
        Covariance of the two columns

        Notes
        -----
        This function applies only to columns containing numerical data.

        Examples
        --------
        Consider Frame *my_frame*, which accesses a frame that contains a single column named *obs*::

            cov = my_frame.covariance(['col_0', 'col_1'])

            print(cov)
                           """)))
  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: ExportCsvArguments)(implicit invocation: Invocation) = 5

  /**
   * Calculate covariance for the specified columns
   *
   * @param invocation information about the user and the circumstances at the time of the call, as well as a function
   *                   that can be called to produce a SparkContext that can be used during this invocation
   * @param arguments input specification for covariance
   * @return value of type declared as the Return type
   */
  override def execute(arguments: ExportCsvArguments)(implicit invocation: Invocation): BoolValue = {

    val frame: SparkFrameData = resolve(arguments.frame)
    // load frame as RDD
    val rdd = frame.data
    return ExportHDFSFunctions.exportToCsvHdfs(rdd, arguments.folderName, arguments.count, arguments.offset, arguments.separator.get)
  }

}