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

package com.intel.intelanalytics.engine.spark.frame

import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.domain.schema._
import org.apache.hadoop.io._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRow

/**
 * This class wraps raw row data adding schema information - this allows for a richer easier to use API.
 *
 * Ideally, every row knows about its schema but this is inefficient when there are many rows.  As an
 * alternative, a single instance of this wrapper can be used to provide the same kind of API.
 *
 * @param schema the schema for the row
 */
class RowWrapper(override val schema: Schema) extends AbstractRow with Serializable {
  require(schema != null, "schema is required")

  @transient override var row: Row = null

  /**
   * Set the data in this wrapper
   * @param row the data to set inside this Wrapper
   * @return this instance
   */
  def apply(row: Row): RowWrapper = {
    this.row = row
    this
  }

}

/**
 * Most implementation belongs here so it can be shared with AbstractVertex and AbstractEdge
 */
trait AbstractRow {

  val schema: Schema
  var row: Row

  /**
   * Determine whether the property exists
   * @param name name of the property
   * @return boolean value indicating whether the property exists
   */
  def hasProperty(name: String): Boolean = {
    try {
      schema.columnIndex(name)
      true
    }
    catch {
      case e: Exception => false
    }
  }

  /**
   * Get property
   * @param columnName name of the property
   * @return property value
   */
  def value(columnName: String): Any = row(schema.columnIndex(columnName))

  /**
   * Get more than one value as a List
   * @param columnNames the columns to get values for
   * @return the values for the columns
   */
  def values(columnNames: List[String] = schema.columnNames): List[Any] = {
    columnNames.map(columnName => value(columnName))
  }

  /**
   * Get property of boolean data type
   * @param columnName name of the property
   * @return property value
   */
  def booleanValue(columnName: String): Boolean = row(schema.columnIndex(columnName)).asInstanceOf[Boolean]

  /**
   * Get property of integer data type
   * @param columnName name of the property
   * @return property value
   */
  def intValue(columnName: String): Int = row(schema.columnIndex(columnName)).asInstanceOf[Int]

  /**
   * Get property of long data type
   * @param columnName name of the property
   * @return property value
   */
  def longValue(columnName: String): Long = row(schema.columnIndex(columnName)).asInstanceOf[Long]

  /**
   * Get property of float data type
   * @param columnName name of the property
   * @return property value
   */
  def floatValue(columnName: String): Float = row(schema.columnIndex(columnName)).asInstanceOf[Float]

  /**
   * Get property of double data type
   * @param columnName name of the property
   * @return property value
   */
  def doubleValue(columnName: String): Double = row(schema.columnIndex(columnName)).asInstanceOf[Double]

  /**
   * Get property of string data type
   * @param columnName name of the property
   * @return property value
   */
  def stringValue(columnName: String): String = row(schema.columnIndex(columnName)).asInstanceOf[String]

  /**
   * True if value for this column is null.
   *
   * (It is non-intuitive but SparkSQL seems to allow null primitives).
   */
  def isNull(columnName: String): Boolean = row.isNullAt(schema.columnIndex(columnName))

  /**
   * Set a value in a column - validates the supplied value is the correct type
   * @param name the name of the column to set
   * @param value the value of the column
   */
  def setValue(name: String, value: Any): Row = {
    validate(name, value)
    setValueIgnoreType(name, value)
  }

  /**
   * Set all of the values for an entire row with validation
   * @param values the values to set
   * @return the row
   */
  def setValues(values: Array[Any]): Row = {
    validate(values)
    setValuesIgnoreType(values)
  }

  def setValues(values: List[Writable]): Row = {
    setValues(values.map(value => WritableRowConversions.writableToValue(value)).toArray)
  }

  /**
   * Validate the supplied value matches the schema for the supplied columnName.
   * @param name column name
   * @param value the value to check
   */
  private def validate(name: String, value: Any): Unit = {
    if (!schema.columnDataType(name).isType(value)) {
      val dataType = DataTypes.dataTypeOfValueAsString(value)
      throw new IllegalArgumentException(s"setting property $name with value $value with an incorrect data type: $dataType")
    }
  }

  private def validate(values: Array[Any]): Unit = {
    require(values.length == schema.columns.length, "number of values must match the number of columns")
    values.zip(schema.columns).foreach { case (value, column) => validate(column.name, value) }
  }

  /**
   * Set the value in a column - don't validate the type
   * @param name the name of the column to set
   * @param value the value of the column
   */
  private def setValueIgnoreType(name: String, value: Any): Row = {
    val position = schema.columnIndex(name)
    val content = row.toArray
    content(position) = value
    //TODO: what is the right way to introduce GenericMutableRow?
    row = new GenericRow(content)
    row
  }

  /**
   * Set all of the values for a row - don't validate type
   * @param values all of the values
   * @return the row
   */
  private def setValuesIgnoreType(values: Array[Any]): Row = {
    //TODO: what is the right way to introduce GenericMutableRow?
    row = new GenericRow(values)
    row
  }

  /**
   * Add a property onto the end of this row.
   *
   * Since this property isn't part of the current schema, no name is supplied.
   *
   * This method changes the schema of the underlying row.
   *
   * @param value the value of the new column
   * @return the row (with a different schema)
   */
  def addValue(value: Any): Row = {
    val content = row.toArray :+ value
    //TODO: what is the right way to introduce GenericMutableRow?
    row = new GenericRow(content)
    row
  }

  /**
   * Add the value if the column name doesn't exist, otherwise set the existing column
   *
   * Note this method may change the schema of the underlying row
   */
  def addOrSetValue(name: String, value: Any): Row = {
    if (!hasProperty(name)) {
      addValue(value)
    }
    else {
      setValueIgnoreType(name, value)
    }
  }

  /**
   * Convert the supplied column from the current type to the supplied dataType
   *
   * This method changes the schema of the underlying row.
   *
   * @param columnName column to change
   * @param dataType new data type to convert existing values to
   * @return the modified row (with a different schema)
   */
  def convertType(columnName: String, dataType: DataType): Row = {
    setValueIgnoreType(columnName, DataTypes.convertToType(value(columnName), dataType))
  }

  /**
   * Get underlying data for this row
   * @return the actual row
   */
  def data: Row = row

  /**
   * Create a new row from the data of the columns supplied
   */
  def valuesAsRow(columnNames: Seq[String] = schema.columnNames): Row = {
    val content = valuesAsArray(columnNames)
    new GenericRow(content)
  }

  /**
   * Select several property values from their names
   * @param names the names of the properties to put into an array
   * @return values for the supplied properties
   */
  def valuesAsArray(names: Seq[String] = schema.columnNames): Array[Any] = {
    val indices = schema.columnIndices(names)
    indices.map(i => row(i)).toArray
  }

  def valueAsWritable(name: String): Writable = {
    WritableRowConversions.valueToWritable(value(name))
  }

  def valueAsWritableComparable(name: String): WritableComparable[_] = {
    WritableRowConversions.valueToWritableComparable(value(name))
  }

  def valuesAsWritable(names: Seq[String] = schema.columnNames): List[Writable] = {
    val indices = schema.columnIndices(names).toList
    indices.map(i => WritableRowConversions.valueToWritable(row(i)))
  }

  /**
   * Create a new row matching the supplied schema adding/dropping columns as needed.
   *
   * @param updatedSchema the new schema to match
   * @return the row matching the new schema
   */
  def valuesForSchema(updatedSchema: Schema): Row = {
    val content = new Array[Any](updatedSchema.columnTuples.length)
    for (columnName <- updatedSchema.columnNames) {
      if (columnName == "_label" && updatedSchema.isInstanceOf[GraphElementSchema]) {
        content(updatedSchema.columnIndex(columnName)) = updatedSchema.asInstanceOf[GraphElementSchema].label
      }
      else if (schema.hasColumnWithType(columnName, updatedSchema.columnDataType(columnName))) {
        content(updatedSchema.columnIndex(columnName)) = value(columnName)
      }
      else if (schema.hasColumn(columnName)) {
        content(updatedSchema.columnIndex(columnName)) = DataTypes.convertToType(value(columnName), updatedSchema.columnDataType(columnName))
      }
      else {
        // it is non-intuitive but even primitives can be null with Rows
        content(updatedSchema.columnIndex(columnName)) = null
      }
    }
    new GenericRow(content)
  }

  // TODO: is tuple conversion nice to have?  either we have a use or we should delete it?
  //  /**
  //   * Get a tuple out of the row
  //   * @param columnName the column to include
  //   * @tparam T the type for the column
  //   * @return a tuple of the supplied columns
  //   */
  //  def toTuple[T](columnName: String): Tuple1[T] = {
  //    Tuple1(value(columnName).asInstanceOf[T])
  //  }
  //
  //  def toTuple[T1, T2](columnName1: String, columnName2: String): (T1, T2) = {
  //    Tuple2(value(columnName1).asInstanceOf[T1], value(columnName2).asInstanceOf[T2])
  //  }
  //
  //  def toTuple[T1, T2, T3](columnName1: String, columnName2: String, columnName3: String): (T1, T2, T3) = {
  //    Tuple3(value(columnName1).asInstanceOf[T1], value(columnName2).asInstanceOf[T2], value(columnName3).asInstanceOf[T3])
  //  }
  //
  //  def toTuple[T1, T2, T3, T4](columnName1: String, columnName2: String, columnName3: String, columnName4: String): (T1, T2, T3, T4) = {
  //    Tuple4(value(columnName1).asInstanceOf[T1], value(columnName2).asInstanceOf[T2], value(columnName3).asInstanceOf[T3], value(columnName4).asInstanceOf[T4])
  //  }
  //
  //  def toTuple[T1, T2, T3, T4, T5](columnName1: String, columnName2: String, columnName3: String, columnName4: String, columnName5: String): (T1, T2, T3, T4, T5) = {
  //    Tuple5(value(columnName1).asInstanceOf[T1], value(columnName2).asInstanceOf[T2], value(columnName3).asInstanceOf[T3], value(columnName4).asInstanceOf[T4], value(columnName5).asInstanceOf[T5])
  //  }

  /**
   * Create a new empty row
   */
  def create(): Row = {
    //TODO: what is the right way to introduce GenericMutableRow?
    val content = new Array[Any](schema.columns.length)
    row = new GenericRow(content)
    row
  }

  /**
   * Create a row with values
   * @param content the values
   * @return the row
   */
  def create(content: Array[Any]): Row = {
    create()
    setValues(content)
  }
}
