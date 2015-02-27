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

package com.intel.graphbuilder.driver.spark.titan

import java.io._

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import com.esotericsoftware.minlog.Log
import com.thinkaurelius.titan.hadoop.FaunusVertex
import org.apache.commons.io.FileUtils

/**
 * Kryo serializer for Titan/Faunus vertices.
 */

class FaunusVertexSerializer extends Serializer[FaunusVertex] {

  /**
   * Reads bytes and returns a new Faunus vertex
   *
   * @param kryo  Kryo serializer
   * @param input Kryo input stream
   * @param inputClass Class of object to return
   * @return Faunux vertex
   */
  def read(kryo: Kryo, input: Input, inputClass: Class[FaunusVertex]): FaunusVertex = {
    val bytes = kryo.readObject(input, classOf[Array[Byte]])
    val inputStream = new DataInputStream(new ByteArrayInputStream(bytes))

    val faunusVertex = new FaunusVertex()
    faunusVertex.readFields(inputStream)
    faunusVertex
  }

  /**
   * Writes Faunus vertex to byte stream
   *
   * @param kryo  Kryo serializer
   * @param output Kryo output stream
   * @param faunusVertex Faunus vertex to serialize
   */
  def write(kryo: Kryo, output: Output, faunusVertex: FaunusVertex): Unit = {
    val outputStream = new ByteArrayOutputStream()
    faunusVertex.write(new DataOutputStream(outputStream))
    kryo.writeObject(output, outputStream.toByteArray)
  }
}
