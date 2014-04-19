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

package org.apache.spark.api.python

import org.apache.spark.rdd.RDD
import java.util.{List => JList, ArrayList => JArrayList, Map => JMap}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.Accumulator
import org.apache.spark.AccumulatorParam
import scala.reflect.ClassTag

/**
 * Wrapper to enable access to private Spark class PythonRDD
 */
class EnginePythonRDD[T: ClassTag](
                                    parent: RDD[T],
                                    command: Array[Byte],
                                    envVars: JMap[String, String],
                                    pythonIncludes: JList[String],
                                    preservePartitioning: Boolean,
                                    pythonExec: String,
                                    broadcastVars: JList[Broadcast[Array[Byte]]],
                                    accumulator: Accumulator[JList[Array[Byte]]])
  extends PythonRDD[T](parent, command, envVars, pythonIncludes,
    preservePartitioning, pythonExec, broadcastVars, accumulator) {

}

class EnginePythonAccumulatorParam()
  extends AccumulatorParam[JList[Array[Byte]]] {
  override def zero(value: JList[Array[Byte]]): JList[Array[Byte]] = new JArrayList
  override def addInPlace(val1: JList[Array[Byte]], val2: JList[Array[Byte]])
  : JList[Array[Byte]] = {
    val1.addAll(val2)
    val1
  }
}
