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

package org.apache.spark.api.python

import org.apache.spark.rdd.RDD
import java.util.{ List => JList, ArrayList => JArrayList, Map => JMap }

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.Accumulator
import org.apache.spark.AccumulatorParam
import scala.reflect.ClassTag
import org.apache.spark.{ SparkException, SparkEnv, AccumulatorParam, Accumulator }
import org.apache.spark.util.Utils
import java.net.Socket
import java.io.{ BufferedOutputStream, DataOutputStream }

/**
 * Wrapper to enable access to private Spark class PythonRDD
 */
class EnginePythonRdd[T: ClassTag](
  parent: RDD[T],
  command: Array[Byte],
  envVars: JMap[String, String],
  pythonIncludes: JList[String],
  preservePartitioning: Boolean,
  pythonExec: String,
  broadcastVars: JList[Broadcast[IAPythonBroadcast]],
  accumulator: Accumulator[JList[Array[Byte]]])
    extends PythonRDD(parent, command, envVars, pythonIncludes,
      preservePartitioning, pythonExec, broadcastVars.asInstanceOf[JList[Broadcast[PythonBroadcast]]], accumulator) {

}

class IAPythonBroadcast extends PythonBroadcast("") {}
/**
 * Internal class that acts as an `AccumulatorParam` for Python accumulators. Inside, it
 * collects a list of pickled strings that we pass to Python through a socket.
 */
//class EnginePythonAccumulatorParam(@transient serverHost: String, serverPort: Int)
//  extends AccumulatorParam[JList[Array[Byte]]] {
//
//  Utils.checkHost(serverHost, "Expected hostname")
//
//  val bufferSize = SparkEnv.get.conf.getInt("spark.buffer.size", 65536)
//
//  override def zero(value: JList[Array[Byte]]): JList[Array[Byte]] = new JArrayList
//
//  override def addInPlace(val1: JList[Array[Byte]], val2: JList[Array[Byte]])
//  : JList[Array[Byte]] = {
//    if (serverHost == null) {
//      // This happens on the worker node, where we just want to remember all the updates
//      val1.addAll(val2)
//      val1
//    } else {
//      // This happens on the master, where we pass the updates to Python through a socket
//      val socket = new Socket(serverHost, serverPort)
//      val in = socket.getInputStream
//      val out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream, bufferSize))
//      out.writeInt(val2.size)
//      for (array <- val2) {
//        out.writeInt(array.length)
//        out.write(array)
//      }
//      out.flush()
//      // Wait for a byte from the Python side as an acknowledgement
//      val byteRead = in.read()
//      if (byteRead == -1) {
//        throw new SparkException("EOF reached before Python server acknowledged")
//      }
//      socket.close()
//      null
//    }
//  }
//}
