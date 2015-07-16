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

package com.intel.taproot.analytics.engine.spark.frame.plugins.statistics.descriptives

import com.intel.taproot.analytics.domain.frame._
import com.intel.taproot.analytics.engine.plugin.{ Invocation, PluginDoc }
import com.intel.taproot.analytics.engine.spark.frame.SparkFrame
import com.intel.taproot.analytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.frame.FrameRdd

// Implicits needed for JSON conversion
import spray.json._
import com.intel.taproot.analytics.domain.DomainJsonProtocol._

/**
 * Parameters
 * ----------
 * column_input : List[Map]
 *   List of Column Input Map
 * Column Input Map: Map
 *   Dictionary must comprise of Column name (str) and any of the 2 optional params top_k (int) and threshold (float)
 */
@PluginDoc(oneLine = "Compute the categorical summary for specified columns in a frame.",
  extended = """Compute the categorical summary of the data in a column.
The returned value is a Map containing categorical summary for each specified column.
For each column, levels which satisfy the top k and/or threshold cutoffs are displayed along
with their frequency and percentage occurrence with respect to the total rows in the dataset.
Missing data is reported when a column value is empty ("") or null. All remaining data is
grouped together in the Other category and its frequency and percentage are reported as well.

User needs to specify the column name and can specify either/both top_k and threshold cutoff.
Specifying top_k parameter displays levels which are in the top k most frequently occuring values
for that column.
Specifying threshold parameter displays levels which are atleast above the threshold percentage
with respect to the total row count
Specifying both of these parameters will do the level pruning first based on top k and next filter
out levels which satisfy the threshold criterion.
Specifying none of these parameters will default the top_k to 10 and threshold to 0.0 i.e. show all
levels which are in Top 10. These defaults can be configured if needed in application.conf.

Examples:

print f.categorical_summary([{'column': 'col1', 'threshold' : 0.5}, {'column' : 'col2', 'top_k' : 1}])
--
{u'categorical_summary': [{u'column': u'a', u'levels': [{u'percentage': 0.6666666666666666, u'frequency': 2,
u'level': u'4'}, {u'percentage': 0.0, u'frequency': 0, u'level': u'Missing'}, {u'percentage': 0.3333333333333333,
u'frequency': 1, u'level': u'Other'}]}, {u'column': u'b', u'levels': [{u'percentage': 0.6666666666666666,
u'frequency': 2, u'level': u'2'}, {u'percentage': 0.0, u'frequency': 0, u'level': u'Missing'},
{u'percentage': 0.3333333333333333, u'frequency': 1, u'level': u'Other'}]}]}

""",
  returns = """categorical_summary: Map
    Dictionary for each column specified in the input dataset
column : str
    Name of input column.
levels : Array of Map
    A list of dictionary which holds information on level, frequency and percentage.""")
class CategoricalSummaryPlugin extends SparkCommandPlugin[CategoricalSummaryArgs, CategoricalSummaryReturn] {

  /**
   * The name of the command, e.g. frame/categorical_summary
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/categorical_summary"

  /**
   * Calculate categorical summary of the specified column.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments Input specification for categorical  summary.
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: CategoricalSummaryArgs)(implicit invocation: Invocation): CategoricalSummaryReturn = {

    val frame: SparkFrame = arguments.frame

    val default_top_k = configuration.getInt("top_k")
    val default_threshold = configuration.getDouble("threshold")

    // Select each column and invoke summary statistics
    val selectedRdds: List[(FrameRdd, CategoricalColumnInput)] =
      arguments.columnInput.map(elem => (frame.rdd.selectColumn(elem.column), elem))

    val output = for { rdd <- selectedRdds }
      yield CategoricalSummaryImpl.getSummaryStatistics(
      rdd._1,
      frame.rowCount.get.asInstanceOf[Double],
      rdd._2.topK,
      rdd._2.threshold,
      default_top_k,
      default_threshold)

    CategoricalSummaryReturn(output)
  }

}

