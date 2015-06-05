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

package com.intel.intelanalytics.engine.spark.frame

import java.io.{ File, PrintWriter }
import java.util

import com.intel.event.EventContext
import com.intel.intelanalytics.component.ClassLoaderAware
import com.intel.intelanalytics.domain.frame.{ FrameReference, FrameEntity }
import com.intel.intelanalytics.domain.frame.{ UdfDependency, Udf }
import com.intel.intelanalytics.domain.schema.{ FrameSchema, DataTypes, Schema }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.SparkContext
import org.apache.spark.api.python.{ IAPythonBroadcast, EnginePythonAccumulatorParam, EnginePythonRdd, PythonBroadcast }
import org.apache.commons.codec.binary.Base64.decodeBase64
import java.util.{ ArrayList => JArrayList, List => JList }

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.bson.types.BasicBSONList
import org.bson.{ BSON, BasicBSONObject }
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import scala.collection.mutable.ArrayBuffer
import com.google.common.io.Files

import scala.collection.JavaConversions._

import scala.reflect.io.{ Directory, Path }

object PythonRddStorage {

  private def decodePythonBase64EncodedStrToBytes(byteStr: String): Array[Byte] = {
    decodeBase64(byteStr)
  }

  //TODO: Use config + UUID rather than hard coded paths.
  private def uploadUdfDependencies(udf: Udf): List[String] = {
    udf.dependencies.map(d => {
      val path = new File(SparkEngineConfig.pythonUdfDependenciesDirectory)
      path.mkdirs() // no check --if false, dirs may already exist; if other problem, will catch during write
      val data = decodePythonBase64EncodedStrToBytes(d.fileContent)
      val fileName = d.fileName.split("/").last
      val fullFileName = SparkEngineConfig.pythonUdfDependenciesDirectory + fileName
      try {
        Files.write(data, new File(fullFileName))
      }
      catch {
        case e: Exception => throw new Exception(s"Unable to upload UDF dependency '$fullFileName'  $e")
      }
      fileName
    })
  }

  def mapWith(data: FrameRdd, udf: Udf, udfSchema: Schema = null, sc: SparkContext): FrameRdd = {
    val newSchema = if (udfSchema == null) { data.frameSchema } else { udfSchema }
    val converter = DataTypes.parseMany(newSchema.columnTuples.map(_._2).toArray)(_)

    val pyRdd = RDDToPyRDD(udf, data.toLegacyFrameRdd, sc)
    val frameRdd = getRddFromPythonRdd(pyRdd, converter)
    FrameRdd.toFrameRdd(newSchema, frameRdd)
  }

  //TODO: fix hardcoded paths
  def uploadFilesToSpark(uploads: List[String], sc: SparkContext): JArrayList[String] = {
    val pythonIncludes = new JArrayList[String]()
    if (uploads != null) {
      for (k <- 0 until uploads.size) {
        sc.addFile(s"file://${SparkEngineConfig.pythonUdfDependenciesDirectory}" + uploads(k))
        pythonIncludes.add(uploads(k))
      }
    }
    pythonIncludes
  }

  /**
   * Convert an iterable into a BasicBSONList for serialiation purposes
   * @param iterable vector object
   */
  def iterableToBsonList(iterable: Iterable[Any]): BasicBSONList = {
    val bsonList = new BasicBSONList
    // BasicBSONList only supports the put operation with a numeric value as the key.
    iterable.zipWithIndex.foreach {
      case (obj: Any, index: Int) => bsonList.put(index, obj)
    }
    bsonList
  }

  def RDDToPyRDD(udf: Udf, rdd: LegacyFrameRdd, sc: SparkContext): EnginePythonRdd[Array[Byte]] = {
    val predicateInBytes = decodePythonBase64EncodedStrToBytes(udf.function)

    // Create an RDD of byte arrays representing bson objects
    val baseRdd: RDD[Array[Byte]] = rdd.map(
      x => {

        val obj = new BasicBSONObject()
        obj.put("array", x.map(value => value match {
          case y: ArrayBuffer[Double] => iterableToBsonList(y)
          case y: Vector[Double] => iterableToBsonList(y)
          case _ => value
        }))
        BSON.encode(obj)
      }
    )

    val pythonExec = SparkEngineConfig.pythonWorkerExec
    val environment = new util.HashMap[String, String]()
    //This is needed to make the python executors put the spark jar (with the pyspark files) on the PYTHONPATH.
    //Without it, only the first added jar (engine.jar) goes on the pythonpath, and since engine.jar has
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
      pyIncludes = uploadFilesToSpark(includes, sc)
    }

    val pyRdd = new EnginePythonRdd[Array[Byte]](
      baseRdd, predicateInBytes, environment,
      pyIncludes, preservePartitioning = true,
      pythonExec = pythonExec,
      broadcastVars, accumulator)
    pyRdd
  }

  def getRddFromPythonRdd(pyRdd: EnginePythonRdd[Array[Byte]], converter: (Array[Any] => Array[Any]) = null): RDD[Array[Any]] = {
    val resultRdd = pyRdd.flatMap(s => {
      //should be BasicBSONList containing only BasicBSONList objects
      val bson = BSON.decode(s)
      val asList = bson.get("array").asInstanceOf[BasicBSONList]
      asList.map(innerList => {
        val asBsonList = innerList.asInstanceOf[BasicBSONList]
        asBsonList.map(value => value match {
          case x: BasicBSONList => x.toArray
          case _ => value
        }).toArray.asInstanceOf[Array[Any]]
      })
    }).map(converter)

    resultRdd
  }
}

/**
 * Loading and saving Python RDD's
 */
class PythonRddStorage(frames: SparkFrameStorage) extends ClassLoaderAware {

  /**
   * Create a Python RDD
   * @param frameRef source frame for the parent RDD
   * @param py_expression Python expression encoded in Python's Base64 encoding (different than Java's)
   * @return the RDD
   */
  def createPythonRDD(frameRef: FrameReference, py_expression: String, sc: SparkContext)(implicit invocation: Invocation): EnginePythonRdd[Array[Byte]] = {
    withMyClassLoader {

      val rdd: LegacyFrameRdd = frames.loadLegacyFrameRdd(sc, frameRef)

      PythonRddStorage.RDDToPyRDD(new Udf(py_expression, null), rdd, sc)
    }
  }
}
