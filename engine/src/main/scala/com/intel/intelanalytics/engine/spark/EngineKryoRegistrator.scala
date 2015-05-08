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

package com.intel.intelanalytics.engine.spark

import com.esotericsoftware.kryo.Kryo
import com.intel.graphbuilder.driver.spark.titan.GraphBuilderKryoRegistrator
import com.intel.intelanalytics.domain.schema.Schema
import com.intel.intelanalytics.engine.Rows.Row
import com.intel.intelanalytics.engine.spark.frame.plugins.classificationmetrics.ClassificationMetrics
import com.intel.intelanalytics.engine.spark.frame.plugins.cumulativedist.CumulativeDistFunctions
import com.intel.intelanalytics.engine.spark.frame.plugins.groupby.{ GroupByAggregationFunctions, GroupByAggregateByKey }
import com.intel.intelanalytics.engine.spark.frame.plugins.load.{ CsvRowParser, LoadRddFunctions, RowParseResult }
import com.intel.intelanalytics.engine.spark.frame.plugins.statistics.descriptives.ColumnStatistics
import com.intel.intelanalytics.engine.spark.frame.plugins.statistics.numericalstatistics.StatisticsRddFunctions
import com.intel.intelanalytics.engine.spark.frame.plugins.statistics.quantiles.QuantilesFunctions
import com.intel.intelanalytics.engine.spark.frame.plugins.topk.TopKRddFunctions
import com.intel.intelanalytics.engine.spark.frame.plugins.{ EntropyRddFunctions, FlattenColumnFunctions }
import com.intel.intelanalytics.engine.spark.frame.{ LegacyFrameRdd, MiscFrameFunctions }
import org.apache.spark.frame.FrameRdd

import org.apache.spark.serializer.KryoRegistrator

/**
 * Register classes that are going to be serialized by Kryo.
 * If you miss a class here, it will likely still work, but registering
 * helps Kryo to go faster.
 * <p>
 * Kryo is 2x to 10x faster than Java Serialization.  In one experiment,
 * with graph building Kryo was 2 hours faster with 23GB of Netflix data.
 * </p>
 * <p>
 * Usage:
 * conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
 * conf.set("spark.kryo.registrator", "com.intel.intelanalytics.engine.spark.EngineKryoRegistrator")
 * </p>
 */
class EngineKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo): Unit = {

    // frame related classes
    kryo.register(classOf[Row])
    kryo.register(classOf[Schema])
    kryo.register(classOf[CsvRowParser])
    kryo.register(classOf[RowParseResult])
    kryo.register(classOf[LegacyFrameRdd])
    kryo.register(classOf[FrameRdd])
    kryo.register(ClassificationMetrics.getClass)
    kryo.register(CumulativeDistFunctions.getClass)
    kryo.register(MiscFrameFunctions.getClass)
    kryo.register(LoadRddFunctions.getClass)
    kryo.register(FlattenColumnFunctions.getClass)
    kryo.register(ColumnStatistics.getClass)
    kryo.register(StatisticsRddFunctions.getClass)
    kryo.register(QuantilesFunctions.getClass)
    kryo.register(TopKRddFunctions.getClass)
    kryo.register(EntropyRddFunctions.getClass)
    kryo.register(GroupByAggregationFunctions.getClass)

    // register GraphBuilder classes
    val gbRegistrator = new GraphBuilderKryoRegistrator()
    gbRegistrator.registerClasses(kryo)
  }
}
