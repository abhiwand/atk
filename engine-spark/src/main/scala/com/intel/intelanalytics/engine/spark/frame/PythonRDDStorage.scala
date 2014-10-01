package com.intel.intelanalytics.engine.spark.frame

import com.intel.intelanalytics.component.ClassLoaderAware
import com.intel.intelanalytics.domain.frame.DataFrame
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.SparkContext
import org.apache.spark.api.python.{ EnginePythonAccumulatorParam, EnginePythonRDD }

import java.util.{ ArrayList => JArrayList, List => JList }

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

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
      val predicateInBytes = decodePythonBase64EncodedStrToBytes(py_expression)

      val baseRdd: RDD[String] = frames.loadFrameRdd(ctx, frameId)
        .map(x => x.map(t => t match {
          case null => JsNull
          case a => a.toJson
        }).toJson.toString)

      val pythonExec = SparkEngineConfig.pythonWorkerExec
      val environment = new java.util.HashMap[String, String]()

      val accumulator = ctx.accumulator[JList[Array[Byte]]](new JArrayList[Array[Byte]]())(new EnginePythonAccumulatorParam())

      val broadcastVars = new JArrayList[Broadcast[Array[Byte]]]()

      val pyRdd = new EnginePythonRDD[String](
        baseRdd, predicateInBytes, environment,
        new JArrayList, preservePartitioning = false,
        pythonExec = pythonExec,
        broadcastVars, accumulator)
      pyRdd
    }
  }

  /**
   * Persists a PythonRDD after python computation is complete to HDFS
   *
   * @param dataFrame DataFrame associated with this RDD
   * @param pyRdd PythonRDD instance
   * @param converter Schema Function converter to convert internals of RDD from Array[String] to Array[Any]
   * @param skipRowCount Skip counting rows when persisting RDD for optimizing speed
   * @return rowCount Number of rows if skipRowCount is false, else 0 (for optimization/transformations which do not alter row count)
   */
  def persistPythonRDD(dataFrame: DataFrame, pyRdd: EnginePythonRDD[String], converter: Array[String] => Array[Any], skipRowCount: Boolean = false): Long = {
    withMyClassLoader {

      val resultRdd = pyRdd.map(s => JsonParser(new String(s)).convertTo[List[List[JsValue]]].map(y => y.map(x => x match {
        case x if x.isInstanceOf[JsString] => x.asInstanceOf[JsString].value
        case x if x.isInstanceOf[JsNumber] => x.asInstanceOf[JsNumber].toString
        case x if x.isInstanceOf[JsBoolean] => x.asInstanceOf[JsBoolean].toString
        case _ => null
      }).toArray))
        .flatMap(identity)
        .map(converter)

      val rowCount = if (skipRowCount) 0 else resultRdd.count()
      //      frames.saveFrameWithoutSchema(ctx, dataFrame, resultRdd)
      frames.saveFrame(dataFrame, new FrameRDD(dataFrame.schema, resultRdd))
      rowCount
    }
  }

  private def decodePythonBase64EncodedStrToBytes(byteStr: String): Array[Byte] = {
    // Python uses different RFC than Java, must correct a couple characters
    // http://stackoverflow.com/questions/21318601/how-to-decode-a-base64-string-in-scala-or-java00
    val corrected = byteStr.map { case '-' => '+'; case '_' => '/'; case c => c }
    new sun.misc.BASE64Decoder().decodeBuffer(corrected)
  }

}
