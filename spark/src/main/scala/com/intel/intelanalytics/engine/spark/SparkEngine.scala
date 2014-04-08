//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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

package com.intel.intelanalytics.engine.spark

import org.apache.spark.{SparkContext, SparkConf, Accumulator}
import org.apache.spark.api.python._
import org.apache.spark.rdd.RDD
import java.util.{List => JList, ArrayList => JArrayList, Map => JMap}
import org.apache.spark.broadcast.Broadcast
import scala.collection.Set
import scala.Predef
import com.intel.intelanalytics.engine.{EngineComponent, RowFunction}

class SparkComponent extends EngineComponent {
  val engine = new SparkEngine {  }
  val sparkHome = System.getProperty("spark.home", "c:\\temp")
  val sparkMaster = System.getProperty("spark.master", "local[4]")

  //Very simpleminded implementation, not ready for multiple users, for example.
  trait SparkEngine extends Engine {

    def config = new SparkConf()
                      .setMaster(sparkMaster)
                      .setAppName("tribeca")
                      .setSparkHome(sparkHome)

    def context() = {
      withMyClassLoader {
        new SparkContext(config = config)
      }
    }

    def withMyClassLoader[T](f: =>T): T = {
      val prior = Thread.currentThread().getContextClassLoader
      try {
        Thread.currentThread()setContextClassLoader(this.getClass.getClassLoader)
        f
      } finally {
        Thread.currentThread().setContextClassLoader(prior)
      }
    }

    def getRddForId(id: Long) : RDD[Array[Byte]] = {
      context().textFile("frame_" + id + ".txt").map(line => line.getBytes("UTF-8"))
    }

    def getPyCommand(function: RowFunction[Boolean]): Array[Byte] = {
      function.definition.getBytes("UTF-8")
    }

    override def dropRows(id: Long, filter: RowFunction[Boolean]): Unit = {
      val rdd = getRddForId(id)
      val command = getPyCommand(filter)
      val pythonExec = "python" //TODO: take from env var or config
      val environment = System.getenv() //TODO - should be empty instead?
      val pyRdd = new EnginePythonRDD[Array[Byte]](
                      rdd, command = command, System.getenv(),
                      new JArrayList, preservePartitioning = true,
                      pythonExec = pythonExec,
                      broadcastVars = new JArrayList[Broadcast[Array[Byte]]](),
                      accumulator = new Accumulator[JList[Array[Byte]]](
                        initialValue = new JArrayList[Array[Byte]](),
                        param = null)
      )
      pyRdd.map(bytes => new String(bytes, "UTF-8")).saveAsTextFile("frame_" + id + "_drop.txt")
    }

    override def dropColumn(id: Long, name: String): Unit = ???

    override def addColumn(id: Long, name: String, map: Option[RowFunction[Any]]): Unit = ???

    override def appendFile(id: Long, name: String, parser: RowFunction[Predef.Map[String, Any]]): Unit = ???
  }

}

