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

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.{ DataFrame, FlattenColumn }
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.LegacyFrameRDD
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.rdd.RDD
import scala.concurrent.ExecutionContext
import com.intel.intelanalytics.domain.schema.{ Schema, DataTypes }
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import java.util.regex.Pattern

/**
 * Take a row with multiple values in a column and 'flatten' it into multiple rows.
 */
class FlattenColumnPlugin extends SparkCommandPlugin[FlattenColumn, DataFrame] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/flatten_column"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = Some(CommandDoc("Flattens cell to multiple rows", Some("""
    Splits cells in the specified column into multiple rows according to a string delimiter.  New
    rows are a full copy of the original row, but the specified column only contains one value.  The
    original row is deleted.

    Parameters
    ----------
    column: str
        The column to be flattened.
    delimiter: str, optional
        The delimiter string, if not specified, a comma is used

    Returns
    -------
    None

    Examples
    --------
    Given that I have a frame accessed by Frame *my_frame* and the frame has two columns *a* and *b*.
    The "original_data"::

        1-"solo,mono,single"
        2-"duo,double"

    I run my commands to bring the data in where I can work on it::

        my_csv = CsvFile("original_data.csv", schema=[('a', int32), ('b', string)],
            delimiter='-')
        # The above command has been split for enhanced readability in some medias.
        my_frame = Frame(source=my_csv)

    I look at it and see::

        my_frame.inspect()

          a:int32   b:string
        /------------------------------/
            1       solo, mono, single
            2       duo, double

    Now, I want to spread out those sub-strings in column *b*::

        my_frame.flatten_column('b')

    Now I check again and my result is::

        my_frame.inspect()

          a:int32   b:str
        /------------------/
            1       solo
            1       mono
            1       single
            2       duo
            2       double


    .. versionadded:: 0.8""")))

  override def numberOfJobs(arguments: FlattenColumn)(implicit invocation: Invocation): Int = 2

  override def numberOfJobs(arguments: FlattenColumn)(implicit invocation: Invocation): Int = 2

  /**
   * Take a row with multiple values in a column and 'flatten' it into multiple rows.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments input specification for column flattening
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: FlattenColumn)(implicit invocation: Invocation): DataFrame = {
    // dependencies (later to be replaced with dependency injection)
    val frames = engine.frames

    // validate arguments
    val frameEntity = frames.expectFrame(arguments.frame.id)
    val columnIndex = frameEntity.schema.columnIndex(arguments.column)
    val delimiter: String = arguments.delimiter.getOrElse(",")

    // run the operation
    val rdd = frames.loadLegacyFrameRdd(sc, frameEntity)
    val flattenedRDD = FlattenColumnFunctions.flattenRddByColumnIndex(columnIndex, delimiter, rdd)

    // save results
    frames.saveLegacyFrame(frameEntity.toReference, new LegacyFrameRDD(frameEntity.schema, flattenedRDD))
  }

}

/**
 * This is a wrapper to encapsulate methods that may need to be serialized to executed on Spark worker nodes.
 * If you don't know what this means please read about Closure Mishap
 * [[http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-part-1-amp-camp-2012-spark-intro.pdf]]
 * and Task Serialization
 * [[http://stackoverflow.com/questions/22592811/scala-spark-task-not-serializable-java-io-notserializableexceptionon-when]]
 */
object FlattenColumnFunctions extends Serializable {

  /**
   * Flatten RDD by the column with specified column index
   * @param index column index
   * @param separator separator for splitting
   * @param rdd RDD for flattening
   * @return new RDD with column flattened
   */
  def flattenRddByColumnIndex(index: Int, separator: String, rdd: RDD[Row]): RDD[Row] = {
    rdd.flatMap(row => flattenColumnByIndex(index, row, separator))
  }

  /**
   * flatten a row by the column with specified column index
   * Eg. for row (1, "dog,cat"), flatten by second column will yield (1,"dog") and (1,"cat")
   * @param index column index
   * @param row row data
   * @param delimiter separator for splitting
   * @return flattened out row/rows
   */
  private[frame] def flattenColumnByIndex(index: Int, row: Array[Any], delimiter: String): Array[Array[Any]] = {
    val splitted = row(index).asInstanceOf[String].split(Pattern.quote(delimiter))
    splitted.map(s => {
      val r = row.clone()
      r(index) = s
      r
    })
  }
}
