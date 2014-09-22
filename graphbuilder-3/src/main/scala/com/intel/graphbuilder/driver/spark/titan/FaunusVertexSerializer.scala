package com.intel.graphbuilder.driver.spark.titan

import java.io._

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.minlog.Log
import com.thinkaurelius.titan.hadoop.FaunusVertex
import org.apache.commons.io.FileUtils

/**
 * Kryo serializer for Faunus vertices.
 */

class FaunusVertexSerializer extends Serializer[FaunusVertex] {

  def read(kryo: Kryo, input: Input, inputClass: Class[FaunusVertex]): FaunusVertex = {
    val bytes = kryo.readObject(input, classOf[Array[Byte]])
    val inputStream = new DataInputStream(new ByteArrayInputStream(bytes))

    val faunusVertex = new FaunusVertex()
    faunusVertex.readFields(inputStream)
    faunusVertex
  }

  def write(kryo: Kryo, output: Output, faunusVertex: FaunusVertex): Unit = {
    val outputStream = new ByteArrayOutputStream()
    faunusVertex.write(new DataOutputStream(outputStream))
    kryo.writeObject(output, outputStream.toByteArray)
  }
}


object FaunusVertexSerializer {

  def main(args: Array[String]) = {
    println("Hello")
    FileUtils.deleteQuietly(new File("/tmp/file.bin"))
    Log.TRACE()
    val kryo = new Kryo()
    //val gbRegistrator = new GraphBuilderKryoRegistrator()
    //gbRegistrator.registerClasses(kryo)



    kryo.register(classOf[FaunusVertex], new FaunusVertexSerializer(), 1000)
    val output = new Output(new FileOutputStream("/tmp/file.bin"))
    val testVertex = new FaunusVertex()
    testVertex.setId(256L)
    kryo.writeObject(output, testVertex)
    //testVertex.write(new DataOutputStream(output.getOutputStream))
    output.close()

    val input = new Input(new FileInputStream("/tmp/file.bin"))
    //val readVertex = new FaunusVertex()
    val readVertex = kryo.readObject(input, classOf[FaunusVertex])
    //readVertex.readFields(new DataInputStream(input.getInputStream))
    println(readVertex)
  }
}