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

package com.intel.intelanalytics.component

import java.net.URLClassLoader
import scala.reflect.io.{File, Path, Directory}
import scala.util.control.NonFatal

object Boot extends App {
  def getClassLoader(archive: String, parent: ClassLoader) : ClassLoader = {
    //TODO: Allow directory to be passed in, or otherwise abstracted?
    //TODO: Make sensitive to actual scala version rather than hard coding.
    val classDirectory : Path  = Directory.Current.get / archive / "target" / "scala-2.10" / "classes"
    if (Directory(classDirectory).exists) {
      println(s"Found class directory at $classDirectory")
      return new URLClassLoader(Array(classDirectory.toURL), parent)
    }
    val jar : Path = Directory.Current.get / "lib" / (archive + ".jar")
    if (File(jar).exists) {
      println(s"Found jar at $jar")
      return new URLClassLoader(Array(jar.toURL), getClass.getClassLoader)
    }
    throw new Exception(s"Could not locate archive $archive")
  }

  def usage() = println("Usage: java -jar launcher.jar <archive> <application>")

  if (args.length != 2) {
    usage()
  } else {
    try {
      val ifaceLoader = getClassLoader("interfaces", getClass.getClassLoader)
      val loader = getClassLoader(args(0), ifaceLoader)
      val klass = loader.loadClass(args(1))
//      val main = klass.getMethod("main")
//      if (main == null) {
//        throw new Exception("No main method on specified class.")
//      }
//      main.invoke(null, Array():Array[String])
      val instance = klass.newInstance().asInstanceOf[Component]
      instance.start(Map.empty)
    } catch {
      case NonFatal(e) => println(e)
    }
  }
}
