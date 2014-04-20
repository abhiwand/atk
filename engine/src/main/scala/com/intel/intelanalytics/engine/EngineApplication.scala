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

class EngineApplication extends Archive {

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

      //TODO: when Engine moves to its own process, it will need to start its own Akka actor system.
      // Until then, running engine.Boot from the launcher will just exit immediately after start.

      //Some testing code, leaving for now in case it's still useful for someone:

//      val ng = engine.engine
//      println("Running test of frame creation and loading")
//      val create = new DataFrameTemplate(name = "test", schema = new Schema(columns = List(("a", "int"))))
//      val f = for {
//        frame <- ng.create(create)
//        _ = println("*************Created")
//        app <- ng.appendFile(frame, "test.csv", new Builtin("line/csv"))
//        _ = println("*************Loaded")
//        rows <- ng.getRows(app.id, offset = 0, count = 10)
//        //filt <- ng.filter(app, new RowFunction[Boolean](language = "python-cloudpickle", definition = "\\x80\\x02ccloud.serialization.cloudpickle\\n_fill_function\\nq\\x00(ccloud.serialization.cloudpickle\\n_make_skel_func\\nq\\x01cnew\\ncode\\nq\\x02(K\\x01K\\x01K\\x03KCU\\x16|\\x00\\x00j\\x00\\x00d\\x01\\x00d\\x02\\x00\\x83\\x02\\x00d\\x03\\x00k\\x04\\x00Sq\\x03(NU\\x01aq\\x04G\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00K\\x01tq\\x05U\\x03getq\\x06\\x85q\\x07U\\x03rowq\\x08\\x85q\\tU\\x07<stdin>q\\nU\\x06myfuncq\\x0bK\\x01U\\x02\\x00\\x01q\\x0c))tq\\rRq\\x0eK\\x00}q\\x0f\\x87q\\x10Rq\\x11}q\\x12N]q\\x13}q\\x14tR."))
//      } yield (app,rows)//, filt)
//      val (meta, data) = Await.result(f, atMost = 60 seconds)
//      println(s"metadata: $meta")
//      data.foreach(row => println(row.map(bytes => new String(bytes)).mkString("\t")))
//    } catch {
//      case NonFatal(e) => {
//        println("ERROR:")
//        println(e)
//        e.printStackTrace()
//        throw e
//      }
    }
  }
}
