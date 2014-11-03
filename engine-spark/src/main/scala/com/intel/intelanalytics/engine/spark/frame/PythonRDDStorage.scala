package com.intel.intelanalytics.engine.spark.frame

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
      PythonRDDStorage.createPythonRDD(frames.loadFrameData(ctx, frameId), py_expression)
    }
  }

  /**
   * Persists a PythonRDD after python computation is complete to HDFS
   *
   * @param dataFrame DataFrame associated with this RDD
   * @param pyRdd PythonRDD instance
   * @param converter Schema Function converter to convert internals of RDD from Array[String] to Array[Any]
   * @param skipRowCount Skip counting rows when persisting RDD for optimizing speed
   * @return rowCount Number of rows if skipRowCount is false, else 0
   *         (for optimization/transformations which do not alter row count)
   */
  def persistPythonRDD(dataFrame: DataFrame,
                       pyRdd: EnginePythonRDD[String],
                       converter: Array[String] => Array[Any],
                       skipRowCount: Boolean = false)(implicit user: UserPrincipal): Long = {
    withMyClassLoader {

      val rdd = PythonRDDStorage.pyRDDToFrameRDD(dataFrame.schema, pyRdd, converter)

      val rowCount = if (skipRowCount) 0 else rdd.count()
      frames.saveFrameData(dataFrame, rdd)
      rowCount
    }
  }
}

object PythonRDDStorage extends ClassLoaderAware {
  /**
   * Create a Python RDD
   * @param frame source frame for the parent RDD
   * @param py_expression Python expression encoded in Python's Base64 encoding (different than Java's)
   * @param user current user
   * @return the RDD
   */
  def createPythonRDD(frame: FrameRDD, py_expression: String)(implicit user: UserPrincipal): EnginePythonRDD[String] = {
    withMyClassLoader {
      val predicateInBytes = decodePythonBase64EncodedStrToBytes(py_expression)

      val baseRdd: RDD[String] = frame.toLegacyFrameRDD.map(x => x.map {
        case null => JsNull
        case a => a.toJson
      }.toJson.toString())

      val pythonExec = SparkEngineConfig.pythonWorkerExec
      val environment = new java.util.HashMap[String, String]()

      implicit val param = new EnginePythonAccumulatorParam()
      val accumulator = frame.context.accumulator[JList[Array[Byte]]](new JArrayList[Array[Byte]]())

      val broadcastVars = new JArrayList[Broadcast[Array[Byte]]]()

      val pyRdd = new EnginePythonRDD[String](
        baseRdd, predicateInBytes, environment,
        new JArrayList, preservePartitioning = false,
        pythonExec = pythonExec,
        broadcastVars, accumulator)
      pyRdd
    }
  }

  def convertWithSchema(pyRdd: EnginePythonRDD[String], schema: Schema): (Array[String] => Array[Any]) = {
    val converter = DataTypes.parseMany(schema.columns.map(_.dataType).toArray)(_)
    converter
  }

  /**
   * Converts a PythonRDD to a FrameRDD
   *
   * @param schema the schema to use for the new RDD
   * @param pyRdd PythonRDD instance
   * @param converter Schema Function converter to convert internals of RDD from Array[String] to Array[Any]
   * @return rowCount Number of rows if skipRowCount is false, else 0 (for optimization/transformations which do not alter row count)
   */
  def pyRDDToFrameRDD(schema: Schema, pyRdd: EnginePythonRDD[String], converter: Array[String] => Array[Any] = null): FrameRDD =
    withMyClassLoader {

      val conv = if (converter == null) {
        convertWithSchema(pyRdd, schema)
      } else {
        converter
      }
      val resultRdd = pyRdd.map(s => JsonParser(new String(s)).convertTo[List[List[JsValue]]].map(y => y.map(x => x match {
        case x if x.isInstanceOf[JsString] => x.asInstanceOf[JsString].value
        case x if x.isInstanceOf[JsNumber] => x.asInstanceOf[JsNumber].toString
        case x if x.isInstanceOf[JsBoolean] => x.asInstanceOf[JsBoolean].toString
        case _ => null
      }).toArray))
        .flatMap(identity)
        .map(conv)
      new LegacyFrameRDD(schema, resultRdd).toFrameRDD()
    }

  private def decodePythonBase64EncodedStrToBytes(byteStr: String): Array[Byte] = {
    // Python uses different RFC than Java, must correct a couple characters
    // http://stackoverflow.com/questions/21318601/how-to-decode-a-base64-string-in-scala-or-java00
    val corrected = byteStr.map { case '-' => '+'; case '_' => '/'; case c => c }
    new sun.misc.BASE64Decoder().decodeBuffer(corrected)
  }
  /**
   * Map the given Python UDF over the rows of the RDD, producing another RDD that should match the given schema.
   */
  def pyMap(frame: FrameRDD, expression: String, schema: Schema)(implicit user: UserPrincipal) =
    pyRDDToFrameRDD(schema, createPythonRDD(frame, expression))

  /**
   * Filter using the given Python UDF over the rows of the RDD, producing another RDD with the same schema.
   */
  def pyFilter(frame: FrameRDD, expression: String)(implicit user: UserPrincipal) =
    //TODO: How should this work exactly?
    pyRDDToFrameRDD(frame.schema, createPythonRDD(frame, expression))
}