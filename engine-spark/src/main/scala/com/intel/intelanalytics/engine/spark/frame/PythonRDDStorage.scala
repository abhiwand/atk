package com.intel.intelanalytics.engine.spark.frame

import java.io.{ File, PrintWriter }
import java.util

import com.intel.event.EventContext
import com.intel.intelanalytics.component.ClassLoaderAware
import com.intel.intelanalytics.domain.frame.FrameEntity
import com.intel.intelanalytics.domain.frame.UdfArgs.{ UdfDependency, Udf }
import com.intel.intelanalytics.domain.schema.{ FrameSchema, DataTypes, Schema }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.SparkContext
import org.apache.spark.api.python.{ IAPythonBroadcast, EnginePythonAccumulatorParam, EnginePythonRDD, PythonBroadcast }
import org.apache.commons.codec.binary.Base64.decodeBase64
import java.util.{ ArrayList => JArrayList, List => JList }

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

import scala.reflect.io.{ Directory, Path }

object PythonRDDStorage {

  private def decodePythonBase64EncodedStrToBytes(byteStr: String): Array[Byte] = {
    decodeBase64(byteStr)
  }

  //TODO: Use config + UUID rather than hard coded paths.
  private def uploadUdfDependencies(udf: Udf): List[String] = {
    val filesToUpload = udf.dependencies.map(f => f.fileName)
    val fileData = udf.dependencies.map(f => f.fileContent)
    var includes = List[String]()

    if (filesToUpload != null) {
      val path = new File("/tmp/intelanalytics/python_udf_deps/")
      if (!path.exists()) {
        if (!path.mkdirs()) throw new Exception(s"Unable to create directory structure for uploading UDF dependencies")
      }

      for {
        i <- 0 until filesToUpload.size
      } {
        val fileToUpload = filesToUpload(i)
        val data = fileData(i)
        val fileName = fileToUpload.split("/").last
        val writer = new PrintWriter(new File("/tmp/intelanalytics/python_udf_deps/" + fileName))
        includes ::= fileName
        writer.write(data)
        writer.close()
      }
    }
    includes
  }

  def mapWith(data: FrameRDD, udf: Udf, udfSchema: Schema = null, ctx: SparkContext): FrameRDD = {
    val newSchema = if (udfSchema == null) { data.frameSchema } else { udfSchema }
    val converter = DataTypes.parseMany(newSchema.columnTuples.map(_._2).toArray)(_)

    val pyRdd = RDDToPyRDD(udf, data.toLegacyFrameRDD, ctx)
    val frameRdd = getRddFromPythonRdd(pyRdd, converter)
    FrameRDD.toFrameRDD(newSchema, frameRdd)
  }

  //TODO: fix hardcoded paths
  def uploadFilesToSpark(uploads: List[String], ctx: SparkContext): JArrayList[String] = {
    val pythonIncludes = new JArrayList[String]()
    if (uploads != null) {
      for (k <- 0 until uploads.size) {
        ctx.addFile("file:///tmp/intelanalytics/python_udf_deps/" + uploads(k))
        pythonIncludes.add(uploads(k))
      }
    }
    pythonIncludes
  }

  def RDDToPyRDD(udf: Udf, rdd: LegacyFrameRDD, ctx: SparkContext): EnginePythonRDD[String] = {
    val predicateInBytes = decodePythonBase64EncodedStrToBytes(udf.function)

    val baseRdd: RDD[String] = rdd
      .map(x => x.map {
        case null => JsNull
        case a => a.toJson
      }.toJson.toString())

    val pythonExec = SparkEngineConfig.pythonWorkerExec
    val environment = new util.HashMap[String, String]()
    //This is needed to make the python executors put the spark jar (with the pyspark files) on the PYTHONPATH.
    //Without it, only the first added jar (engine-spark.jar) goes on the pythonpath, and since engine-spark.jar has
    //more than 65563 files, python can't import pyspark from it (see SPARK-1520 for details).
    //The relevant code in the Spark core project is in PythonUtils.scala. This code must use, e.g. the same
    //version number for py4j that Spark's PythonUtils uses.

    //TODO: Refactor Spark's PythonUtils to better support this use case
    val sparkPython: Path = (SparkEngineConfig.sparkHome: Path) / "python"
    environment.put("PYTHONPATH",
      Seq(sparkPython,
        sparkPython / "lib" / "py4j-0.8.2.1-src.zip",
        //Support dev machines without installing python packages
        //TODO: Maybe hide behind a flag?
        Directory.Current.get / "python").map(_.toString).mkString(":"))

    val accumulator = rdd.sparkContext.accumulator[JList[Array[Byte]]](new JArrayList[Array[Byte]]())(new EnginePythonAccumulatorParam())
    val broadcastVars = new JArrayList[Broadcast[IAPythonBroadcast]]()

    var pyIncludes = new JArrayList[String]()

    if (udf.dependencies != null) {
      val includes = uploadUdfDependencies(udf)
      pyIncludes = uploadFilesToSpark(includes, ctx)
    }

    val pyRdd = new EnginePythonRDD[String](
      baseRdd, predicateInBytes, environment,
      pyIncludes, preservePartitioning = true,
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

      PythonRDDStorage.RDDToPyRDD(new Udf(py_expression, null), rdd, ctx)
    }
  }
}
