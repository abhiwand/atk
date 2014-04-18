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
import java.lang.String
import scala.util.control.NonFatal
import com.intel.intelanalytics.component.Component
import com.intel.intelanalytics.domain.{DataFrameTemplate, Schema, DataFrame}

import scala.concurrent.{Await, ExecutionContext}
import ExecutionContext.Implicits.global
import scala.concurrent.duration._

class Boot extends Component {

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

      val ng = engine.engine
//      val command = new String("ccloud.serialization.cloudpickle\\n_fill_function\\np0\\n(ccloud.serialization.cloudpickle\\n_make_skel_func\\np1\\n(cnew\\ncode\\np2\\n(I1\\nI1\\nI2\\nI67\\nS'|\\\\x00\\\\x00d\\\\x01\\\\x00k\\\\x00\\\\x00S'\\np3\\n(NI5\\ntp4\\n(t(S'x'\\np5\\ntp6\\nS'<stdin>'\\np7\\nS'<lambda>'\\np8\\nI1\\nS''\\np9\\n(t(ttp10\\nRp11\\nI0\\n(dp12\\ntp13\\nRp14\\n(dp15\\nN(lp16\\n(dp17\\ntR.")
      val command = new String("�\u0002(cpyspark.cloudpickle\n_modules_to_main\nq]q\u0001U\n     pyspark.rddq\u0002a�q\u0003R1cpyspark.cloudpickle\n_fill_function\nq\u0004(cpyspark.cloudpickle\n_make_skel_func\nqcnew\ncode\nq\u0006(K\u0002K\u0002K\u0002K\u0013U\n�|\u0001�\u0001SqN�)U\u0001sq\titeratorq\n�q\n  U0/home/joyeshmi/spark-0.9.1/python/pyspark/rdd.pyq\nK�UqU\u0001fq�q\u0010)tq\u0011Rq\u0012K\u0001}q\u0013�q\u0014Rq\u0015}q\u0016N]q\u0017(h]q\u0018h\u0002a�q\u0019R1h\u0004(hh\u0006(K\u0001K\u0002K\u0002K3U4d\u0001}\u0001x'|\u0001�jo\u0019\u0001t|�\u0001V\u0001|\u0001d\u00027}\u0001q\t\u0001WdSq\u001ANKK\u0001�q\u001BU\u0004nextq\u001C�q\u001Dh\nUtakenq\u001E�q\u001FU0/home/joyeshmi/spark-0.9.1/python/pyspark/rdd.pyq U\n                                                                takeUpToNumq!M�\u0002U\n\u0001\u0006\u0001\u0003\n \u0001q\"U\u0003numq#�q$)tq%Rq&K\u0001h\u0013�q'Rq(}q)N]q*K\u0003a}q+tRa}q,tRcpyspark.serializers\nUTF8Deserializer\nq-)�q.}q/bcpyspark.serializers\nBatchedSerializer\nq0)�q1}q2(U\tbatchSizeq3M\u0004U\nserializerq4cpyspark.serializers\nPickleSerializer\nq5)�q6}q7bub�q8.")
      println("Running test of frame creation and loading")
      val create = new DataFrameTemplate(name = "test", schema = new Schema(columns = List(("a", "int"))))
      println("********Serialized(Pickled) Command*******")
      println(command)

      val f = for {
        frame <- ng.create(create)
        _ = println("*************Created")
        app <- ng.appendFile(frame, "test.csv", new Builtin("line/csv"))
        _ = println("*************Loaded")
        rows <- ng.getRows(app.id, offset = 0, count = 10)
        _ = println("*************Got 10 rows")

        filter <- ng.filter(app, new RowFunction[Boolean](language = "python-cloudpickle", definition = command))
//        filter <- ng.filter(app, new RowFunction[Boolean](language = "python-cloudpickle", definition = "ccloud.serialization.cloudpickle\\n_fill_function\\np0\\n(ccloud.serialization.cloudpickle\\n_make_skel_func\\np1\\n(cnew\\ncode\\np2\\n(I1\\nI1\\nI2\\nI67\\nS'|\\\\x00\\\\x00d\\\\x01\\\\x00k\\\\x00\\\\x00S'\\np3\\n(NI5\\ntp4\\n(t(S'x'\\np5\\ntp6\\nS'<stdin>'\\np7\\nS'<lambda>'\\np8\\nI1\\nS''\\np9\\n(t(ttp10\\nRp11\\nI0\\n(dp12\\ntp13\\nRp14\\n(dp15\\nN(lp16\\n(dp17\\ntR."))
        _ = println("*************Filtering")
        filter_rows <- ng.getRows(filter.id, offset = 0, count = 10)
        _ = println("*************Getting Filter Rows")
      } yield (app,rows,filter,filter_rows)
      val (meta, data, filt, filt_rows) = Await.result(f, atMost = 60 seconds)
      println(s"metadata: $meta")
      data.foreach(row => println(row.map(bytes => new String(bytes)).mkString("\t")))
      filt_rows.foreach(row => println(row.map(bytes => new String(bytes)).mkString("\t")))
    } catch {
      case NonFatal(e) => {
        println("ERROR:")
        println(e)
        e.printStackTrace()
        throw e
      }
    }
  }
}
