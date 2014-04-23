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

package com.intel.intelanalytics.engine

import scala.reflect.io.Directory
import java.net.URLClassLoader
import scala.util.control.NonFatal
import com.intel.intelanalytics.component.{Archive}
import com.intel.intelanalytics.domain.{DataFrameTemplate, Schema, DataFrame}

import scala.concurrent.{Await, ExecutionContext}
import ExecutionContext.Implicits.global
import scala.concurrent.duration._
import com.intel.intelanalytics.shared.EventLogging

class EngineApplication extends Archive with EventLogging {

  var engine : EngineComponent with FrameComponent = null

  def get[T] (descriptor: String) = {
    descriptor match {
      case "engine" => engine.engine.asInstanceOf[T]
      case _ => throw new IllegalArgumentException(s"No suitable implementation for: '$descriptor'")
    }
  }

  def stop() = {}

  def start(configuration: Map[String, String]) = {

    try {
      //TODO: when Engine moves to its own process, it will need to start its own Akka actor system.

      val sparkLoader = {
        com.intel.intelanalytics.component.Boot.getClassLoader("engine-spark")
      }

      engine = {
        withLoader(sparkLoader) {
          val class_ = sparkLoader.loadClass("com.intel.intelanalytics.engine.spark.SparkComponent")
          val instance = class_.newInstance()
          instance.asInstanceOf[EngineComponent with FrameComponent]
        }
      }

//      val ng = engine.engine
//      println("Running test of frame creation and loading")
//      val create = new DataFrameTemplate(name = "test", schema = new Schema(columns = List(("a", "int"), ("b", "int"), ("c", "int"))))
//      val f = for {
//        frame <- ng.create(create)
//        _ = println("*************Created")
//        app <- ng.appendFile(frame, "test.csv",new Builtin("line/csv"))
//        _ = println("*************Loaded")
//        rows <- ng.getRows(app.id, offset = 0, count = 10)
//        filt <- ng.filter(app, "sample")
//        filter_rows <- ng.getRows(app.id, offset = 0, count = 10)
//      } yield (app,rows, filt, filter_rows)
//      val (meta, data, fil, fil_rows) = Await.result(f, atMost = 60 seconds)
//      println(s"metadata: $meta")
//      data.foreach(row => println(row.map(bytes => new String(bytes)).mkString("|")))
//      fil_rows.foreach(row => println(row.map(bytes => new String(bytes)).mkString("|")))

    } catch {
      case NonFatal(e) => {
        error("An error occurred while starting the engine.", exception = e)
        throw e
      }
    }
  }
}
