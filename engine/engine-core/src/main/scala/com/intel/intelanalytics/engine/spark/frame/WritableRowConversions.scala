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
