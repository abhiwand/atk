package com.intel.intelanalytics.engine.spark.frame

import java.util

import com.intel.intelanalytics.component.ClassLoaderAware
import com.intel.intelanalytics.domain.frame.DataFrame
import com.intel.intelanalytics.domain.schema.{ DataTypes, Schema }
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

  def pyMappish(data: FrameRDD, pyExpression: String, schema: Schema = null): FrameRDD = {
    val newSchema = if (schema == null) { data.schema } else { schema }
    val converter = DataTypes.parseMany(newSchema.columnTuples.map(_._2).toArray)(_)

    val pyRdd = RDDToPyRDD(pyExpression, data.toLegacyFrameRDD)
    val frameRdd = getRddFromPythonRdd(pyRdd, converter)
    new LegacyFrameRDD(newSchema, frameRdd).toFrameRDD()
  }

  def RDDToPyRDD(py_expression: String, rdd: LegacyFrameRDD): EnginePythonRDD[String] = {
    val predicateInBytes = decodePythonBase64EncodedStrToBytes(py_expression)

    val baseRdd: RDD[String] = rdd
      .map(x => x.map(t => t match {
        case null => JsNull
        case a => a.toJson
      }).toJson.toString)

    val pythonExec = SparkEngineConfig.pythonWorkerExec
    val environment = new util.HashMap[String, String]()

    val accumulator = rdd.sparkContext.accumulator[JList[Array[Byte]]](new JArrayList[Array[Byte]]())(new EnginePythonAccumulatorParam())

    val broadcastVars = new JArrayList[Broadcast[Array[Byte]]]()

    val pyRdd = new EnginePythonRDD[String](
      baseRdd, predicateInBytes, environment,
      new JArrayList, preservePartitioning = false,
      pythonExec = pythonExec,
      broadcastVars, accumulator)
    pyRdd
  }

  def getRddFromPythonRdd(pyRdd: EnginePythonRDD[String], converter: (Array[String]) => Array[Any] = null): RDD[Array[Any]] = {
    val resultRdd = pyRdd.map(s => JsonParser(new String(s)).convertTo[List[List[JsValue]]].map(y => y.map(x => x match {
      case x if x.isInstanceOf[JsString] => x.asInstanceOf[JsString].value
      case x if x.isInstanceOf[JsNumber] => x.asInstanceOf[JsNumber].toString
      case x if x.isInstanceOf[JsBoolean] => x.asInstanceOf[JsBoolean].toString
      case _ => null
    }).toArray))
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
   * @param user current user
   * @return the RDD
   */
  def createPythonRDD(frameId: Long, py_expression: String, ctx: SparkContext)(implicit user: UserPrincipal): EnginePythonRDD[String] = {
    withMyClassLoader {

      val rdd: LegacyFrameRDD = frames.loadLegacyFrameRdd(ctx, frameId)

      PythonRDDStorage.RDDToPyRDD(py_expression, rdd)
    }
  }
}
