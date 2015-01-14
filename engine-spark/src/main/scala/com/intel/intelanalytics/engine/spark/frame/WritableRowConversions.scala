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
      case null => new NullWritable
      case i: Int => new IntWritable(i)
      case l: Long => new LongWritable(l)
      case f: Float => new FloatWritable(f)
      case d: Double => new DoubleWritable(d)
      case s: String => new Text(s)
      case _ => throw new RuntimeException(s"${value.getClass.getName} is not yet implemented")
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
