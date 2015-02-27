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

package com.intel.intelanalytics.engine.spark.frame

import org.apache.hadoop.io._

/**
 * Convert supported dataTypes to/from Hadoop Writables
 */
object WritableRowConversions {

  /**
   * Get a Writable for a Row value
   *
   * @param value the Row value
   * @return a Hadoop Writable
   */
  def valueToWritable(value: Any): Writable = {
    value match {
      // need to support all DataTypes here
      case null => NullWritable.get()
      case i: Int => new IntWritable(i)
      case l: Long => new LongWritable(l)
      case f: Float => new FloatWritable(f)
      case d: Double => new DoubleWritable(d)
      case s: String => new Text(s)
      case _ => throw new RuntimeException(s"${value.getClass.getName} is not yet implemented")
    }
  }

  def valueToWritableComparable(value: Any): WritableComparable[_] = {
    val writable = valueToWritable(value)
    writable match {
      case wc: WritableComparable[_] => wc
      case _ => throw new IllegalArgumentException(s"Type ${value.getClass.getName} converts to ${writable.getClass.getName} which is NOT a WritableComparable")
    }
  }

  /**
   * Convert a Writable to a Row value
   * @param writable
   * @return a Row value
   */
  def writableToValue(writable: Writable): Any = {
    writable match {
      // need to support all DataTypes here
      case n: NullWritable => null
      case i: IntWritable => i.get()
      case l: LongWritable => l.get()
      case f: FloatWritable => f.get()
      case d: DoubleWritable => d.get()
      case s: Text => s.toString
      case _ => throw new RuntimeException(s"${writable.getClass.getName} is not yet implemented")
    }
  }
}
