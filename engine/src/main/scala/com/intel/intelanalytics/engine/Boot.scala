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
import com.intel.intelanalytics.component.Component

class Boot extends Component {

  //  val engineLoader = {
  //    val thisJar = Directory.Current.get / "lib" / "engine.jar"
  //    val loader = new URLClassLoader(Array(thisJar.toURL), getClass.getClassLoader.getParent)
  //    loader
  //  }

  def stop() = {}
  def start(configuration: Map[String, String]) = {
    val sparkLoader = {
      com.intel.intelanalytics.component.Boot.getClassLoader("spark")
    }

    val engine = {
      println("Constructing engine")
      //    val previousLoader = Thread.currentThread().getContextClassLoader
      //    Thread.currentThread().setContextClassLoader(sparkLoader)
      //    try {
      //      val actorRefFactory = sparkLoader.loadClass("akka.remote.RemoteActorRefProvider")
      //      println("Found remote actor provider")

      val class_ = sparkLoader.loadClass("com.intel.intelanalytics.engine.spark.SparkComponent")
      val instance = class_.newInstance()
      instance.asInstanceOf[EngineComponent]
      //    } catch {
      //      case e: Exception => {
      //        var ex:Throwable = e
      //        do {
      //          println(ex)
      //          ex = ex.getCause()
      //        } while(ex != null)
      //        throw(e)
      //      }
      //    } finally {
      //      Thread.currentThread().setContextClassLoader(previousLoader)
      //    }
    }
    println("OK")
    //readLine("Press enter")
    try {
      val actorRefFactory = sparkLoader.loadClass("akka.remote.RemoteActorRefProvider")
      println("Found remote actor provider")
      engine.engine.dropRows(1, new RowFunction[Boolean](language = "python-cloudpickle", definition = "\\x80\\x02ccloud.serialization.cloudpickle\\n_fill_function\\nq\\x00(ccloud.serialization.cloudpickle\\n_make_skel_func\\nq\\x01cnew\\ncode\\nq\\x02(K\\x01K\\x01K\\x03KCU\\x16|\\x00\\x00j\\x00\\x00d\\x01\\x00d\\x02\\x00\\x83\\x02\\x00d\\x03\\x00k\\x04\\x00Sq\\x03(NU\\x01aq\\x04G\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00K\\x01tq\\x05U\\x03getq\\x06\\x85q\\x07U\\x03rowq\\x08\\x85q\\tU\\x07<stdin>q\\nU\\x06myfuncq\\x0bK\\x01U\\x02\\x00\\x01q\\x0c))tq\\rRq\\x0eK\\x00}q\\x0f\\x87q\\x10Rq\\x11}q\\x12N]q\\x13}q\\x14tR."))

    } catch {
      case NonFatal(e) => e.printStackTrace()
    }
  }
}
