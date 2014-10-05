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

