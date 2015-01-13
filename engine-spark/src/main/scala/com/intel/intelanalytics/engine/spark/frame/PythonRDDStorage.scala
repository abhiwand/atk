package com.intel.intelanalytics.engine.spark.frame

import java.io.{ File, PrintWriter }
import java.util

import com.intel.event.EventContext
import com.intel.intelanalytics.component.ClassLoaderAware
import com.intel.intelanalytics.domain.frame.FrameEntity
import com.intel.intelanalytics.domain.frame.UdfArgs.Udf
import com.intel.intelanalytics.domain.schema.{ FrameSchema, DataTypes, Schema }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.SparkContext
import org.apache.spark.api.python.{ EnginePythonAccumulatorParam, EnginePythonRDD }
import org.apache.commons.codec.binary.Base64.decodeBase64

import java.util.{ ArrayList => JArrayList, List => JList }

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

object PythonRDDStorage {

  private def decodePythonBase64EncodedStrToBytes(byteStr: String): Array[Byte] = {
    decodeBase64(byteStr)
  }

  def mapWith(data: FrameRDD, udf: Udf, ctx: SparkContext, schema: Schema = null): FrameRDD = {
    val newSchema = if (schema == null) { data.frameSchema } else { schema }
    val converter = DataTypes.parseMany(newSchema.columnTuples.map(_._2).toArray)(_)

    val filesToUpload = udf.dependencies.map(f => f.filename)
    val fileData = udf.dependencies.map(f => f.fileContent)
    var includes = List[String]()

    if (filesToUpload != null) {
      for {
        i <- 0 until filesToUpload.size
      } {
        val fileToUpload = filesToUpload(i)
        val data = fileData(i)
        val fileName = fileToUpload.split("/").last
        val writer = new PrintWriter(new File("/tmp/" + fileName))
        includes ::= fileName
        writer.write(data)
        writer.close()
      }
    }

    val pythonIncludes = new JArrayList[String]()

    if (includes != null) {
      for (k <- 0 until includes.size) {
        ctx.addFile("file:///tmp/" + includes(k))
        pythonIncludes.add(includes(k))
      }
    }

    val pyRdd = RDDToPyRDD(udf.function, pythonIncludes, data.toLegacyFrameRDD)
    val frameRdd = getRddFromPythonRdd(pyRdd, converter)
    new LegacyFrameRDD(newSchema, frameRdd).toFrameRDD()
  }

  def RDDToPyRDD(py_expression: String, pyIncludes: JList[String], rdd: LegacyFrameRDD): EnginePythonRDD[String] = {
    val predicateInBytes = decodePythonBase64EncodedStrToBytes(py_expression)

    val baseRdd: RDD[String] = rdd
      .map(x => x.map {
        case null => JsNull
        case a => a.toJson
      }.toJson.toString())

    val pythonExec = SparkEngineConfig.pythonWorkerExec
    val environment = new util.HashMap[String, String]()

    val accumulator = rdd.sparkContext.accumulator[JList[Array[Byte]]](new JArrayList[Array[Byte]]())(new EnginePythonAccumulatorParam())

    val broadcastVars = new JArrayList[Broadcast[Array[Byte]]]()

    val pyRdd = new EnginePythonRDD[String](
      baseRdd, predicateInBytes, environment,
      pyIncludes, preservePartitioning = false,
      pythonExec = pythonExec,
      broadcastVars, accumulator)
    pyRdd
  }

  def getRddFromPythonRdd(pyRdd: EnginePythonRDD[String], converter: (Array[Any] => Array[Any]) = null): RDD[Array[Any]] = {
    val resultRdd = pyRdd.map(s => JsonParser(new String(s)).convertTo[List[List[JsValue]]].map(y => y.map(x => x match {
      case x if x.isInstanceOf[JsString] => x.asInstanceOf[JsString].value
      case x if x.isInstanceOf[JsNumber] => x.asInstanceOf[JsNumber].value
      case x if x.isInstanceOf[JsBoolean] => x.asInstanceOf[JsBoolean].toString
      case _ => null
    }).toArray.asInstanceOf[Array[Any]]))
      .flatMap(identity)
      .map(converter)
    resultRdd
  }
}

/**
 * Loading and saving Python RDD's
 */
class PythonRDDStorage(frames: SparkFrameStorage) extends ClassLoaderAware {

  /**
   * Create a Python RDD
   * @param frameId source frame for the parent RDD
   * @param py_expression Python expression encoded in Python's Base64 encoding (different than Java's)
   * @return the RDD
   */
  def createPythonRDD(frameId: Long, py_expression: String, ctx: SparkContext)(implicit invocation: Invocation): EnginePythonRDD[String] = {
    withMyClassLoader {

      val rdd: LegacyFrameRDD = frames.loadLegacyFrameRdd(ctx, frameId)

      PythonRDDStorage.RDDToPyRDD(py_expression, new JArrayList, rdd)
    }
  }
}
