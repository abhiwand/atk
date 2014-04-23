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
import scala.reflect.ClassTag
import org.apache.spark.{SparkException, SparkEnv, AccumulatorParam, Accumulator}
import org.apache.spark.util.Utils
import java.net.Socket
import java.io.{BufferedOutputStream, DataOutputStream}

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


//class EnginePythonAccumulatorParam()
//  extends AccumulatorParam[JList[Array[Byte]]] {
//  override def zero(value: JList[Array[Byte]]): JList[Array[Byte]] = new JArrayList
//  override def addInPlace(val1: JList[Array[Byte]], val2: JList[Array[Byte]])
//  : JList[Array[Byte]] = {
//    val1.addAll(val2)
//    val1
//  }
//}




/**
 * Internal class that acts as an `AccumulatorParam` for Python accumulators. Inside, it
 * collects a list of pickled strings that we pass to Python through a socket.
 */
class EnginePythonAccumulatorParam(@transient serverHost: String, serverPort: Int)
  extends AccumulatorParam[JList[Array[Byte]]] {

  Utils.checkHost(serverHost, "Expected hostname")

  val bufferSize = SparkEnv.get.conf.getInt("spark.buffer.size", 65536)

  override def zero(value: JList[Array[Byte]]): JList[Array[Byte]] = new JArrayList

  override def addInPlace(val1: JList[Array[Byte]], val2: JList[Array[Byte]])
  : JList[Array[Byte]] = {
    if (serverHost == null) {
      // This happens on the worker node, where we just want to remember all the updates
      val1.addAll(val2)
      val1
    } else {
      // This happens on the master, where we pass the updates to Python through a socket
      val socket = new Socket(serverHost, serverPort)
      val in = socket.getInputStream
      val out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream, bufferSize))
      out.writeInt(val2.size)
      for (array <- val2) {
        out.writeInt(array.length)
        out.write(array)
      }
      out.flush()
      // Wait for a byte from the Python side as an acknowledgement
      val byteRead = in.read()
      if (byteRead == -1) {
        throw new SparkException("EOF reached before Python server acknowledged")
      }
      socket.close()
      null
    }
  }
}
