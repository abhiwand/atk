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

package com.intel.intelanalytics.engine.spark.frame.plugins.bincolumn

import com.intel.intelanalytics.UnitReturn
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.domain.schema.{ Schema, DataTypes }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.{ FrameRDD, SparkFrameData, LegacyFrameRDD }
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal

import scala.concurrent.{ Await, ExecutionContext }

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Column values into bins.
 *
 * Two types of binning are provided: equalwidth and equaldepth.
 *
 * Equal width binning places column values into bins such that the values in each bin fall within the same
 * interval and the interval width for each bin is equal.
 *
 * Equal depth binning attempts to place column values into bins such that each bin contains the same number
 * of elements
 */
class BinColumnPlugin extends SparkCommandPlugin[BinColumn, DataFrame] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/bin_column"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = Some(CommandDoc("Bin a particular columnNone", Some("""
    Summarize rows of data based on the value in a single column.
    Two types of binning are provided: `equalwidth` and `equaldepth`.

    *   Equal width binning places column values into bins such that the values in each bin fall within
        the same interval and the interval width for each bin is equal.
    *   Equal depth binning attempts to place column values into bins such that each bin contains the
        same number of elements.
        For :math:`n` bins of a column :math:`C` of length :math:`m`, the bin number is determined by:

        .. math::

            ceiling \\left( n * \\frac {f(C)}{m} \\right)

        where :math:`f` is a tie-adjusted ranking function over values of :math:`C`.
        If there are multiples of the same value in :math:`C`, then their tie-adjusted rank is the
        average of their ordered rank values.

    Parameters
    ----------
    column_name : str
        The column whose values are to be binned.

    num_bins : int
        The maximum number of bins.

    bin_type : str (optional)
        The binning algorithm to use ['equalwidth' | 'equaldepth'].

    bin_column_name : str (optional)
        The name for the new binned column. If unassigned, bin_column_name defaults to '<column_name>_binned'

    Notes
    -----
    1)  Unicode in column names is not supported and will likely cause the drop_frames() function
        (and others) to fail!
    #)  The num_bins parameter is considered to be the maximum permissible number of bins because the
        data may dictate fewer bins.
        With equal depth binning, for example, if the column to be binned has 10 elements with
        only 2 distinct values and the *num_bins* parameter is greater than 2, then the number of
        actual number of bins will only be 2.
        This is due to a restriction that elements with an identical value must belong to the same bin.


    Examples
    --------
    For this example, we will use a frame with column *a* accessed by a Frame object *my_frame*::

        my_frame.inspect( n=11 )

          a:int32
        /---------/
            1
            1
            2
            3
            5
            8
           13
           21
           34
           55
           89

    Modify the frame with a column showing what bin the data is in.
    The data should be separated into a maximum of five bins and the bins should be *equalwidth*::

        my_frame.bin_column('a', 5, 'equalwidth', 'aEWBinned')
        my_frame.inspect( n=11 )

          a:int32     aEWBinned:int32
        /-----------------------------/
           1                   1
           1                   1
           2                   1
           3                   1
           5                   1
           8                   1
          13                   1
          21                   2
          34                   2
          55                   4
          89                   5

    Modify the frame with a column showing what bin the data is in.
    The data should be separated into a maximum of five bins and the bins should be *equaldepth*::


        my_frame.bin_column('a', 5, 'equaldepth', 'aEDBinned')
        my_frame.inspect( n=11 )

          a:int32     aEDBinned:int32
        /-----------------------------/
           1                   1
           1                   1
           2                   1
           3                   2
           5                   2
           8                   3
          13                   3
          21                   4
          34                   4
          55                   5
          89                   5

    .. versionadded:: 0.8""")))

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: BinColumn)(implicit invocation: Invocation) = arguments.binType match {
    case Some("equaldepth") => 8
    case _ => 7
  }

  /**
   * Column values into bins.
   *
   * Two types of binning are provided: equalwidth and equaldepth.
   *
   * Equal width binning places column values into bins such that the values in each bin fall within the same
   * interval and the interval width for each bin is equal.
   *
   * Equal depth binning attempts to place column values into bins such that each bin contains the same number
   * of elements
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: BinColumn)(implicit invocation: Invocation): DataFrame = {
    val frame: SparkFrameData = resolve(arguments.frame)
    val columnIndex = frame.meta.schema.columnIndex(arguments.columnName)
    val columnType = frame.meta.schema.columnDataType(arguments.columnName)
    require(columnType.isNumerical, s"Invalid column ${arguments.columnName} for bin column.  Expected a numerical data type, but got $columnType.")
    val binColumnName = arguments.binColumnName.getOrElse(frame.meta.schema.getNewColumnName(arguments.columnName + "_binned"))
    if (frame.meta.schema.hasColumn(binColumnName))
      throw new IllegalArgumentException(s"Duplicate column name: ${arguments.binColumnName}")
    val binType = arguments.binType.getOrElse("equalwidth")
    require(binType == "equalwidth" || binType == "equaldepth", "bin type must be 'equalwidth' or 'equaldepth', not " + binType)

    // run the operation and save results
    val updatedSchema = frame.meta.schema.addColumn(binColumnName, DataTypes.int32)
    val rdd = frame.data
    val binnedRdd = DiscretizationFunctions.bin(columnIndex, binType, arguments.numBins, rdd.toLegacyFrameRDD)
    save(new SparkFrameData(frame.meta.withSchema(updatedSchema), FrameRDD.toFrameRDD(updatedSchema, binnedRdd))).meta
  }
}
