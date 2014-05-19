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

import java.net.{ URL, URLClassLoader }
import scala.reflect.io.{ File, Path, Directory }
import scala.util.control.NonFatal
import scala.collection.mutable
import PartialFunction._

object Boot extends App {

  //Scalaz also provides this, but we don't want a scalaz dependency in the launcher
  implicit class RichBoolean(val b: Boolean) extends AnyVal {
    final def option[A](a: => A): Option[A] = if (b) Some(a) else None
  }

  val loaders = new mutable.HashMap[String, ClassLoader] with mutable.SynchronizedMap[String, ClassLoader] {}

  val archives = new mutable.HashMap[String, Archive] with mutable.SynchronizedMap[String, Archive] {}

  def buildArchive(archive: String, className: String): Archive = {
    val loader = getClassLoader(archive)
    val klass = loader.loadClass(className)
    val thread = Thread.currentThread()
    val prior = thread.getContextClassLoader
    try {
      thread.setContextClassLoader(loader)
      val instance = klass.newInstance().asInstanceOf[Archive]
      instance.start(Map.empty)
      archives += ((archive + ":" + className) -> instance)
      instance
    }
    finally {
      thread.setContextClassLoader(prior)
    }
  }

  def getArchive(archive: String, className: String): Archive = {
    archives.getOrElse(archive + ":" + className, buildArchive(archive, className))
  }

  def getClassLoader(archive: String): ClassLoader = {
    loaders.getOrElse(archive, buildClassLoader(archive, interfaces))
  }

  /**
   * Return the jar file location for the specified archive
   * @param archive archive to search
   * @param f function for searching code paths that contain the archive
   */
  def getJar(archive: String, f: String => Array[URL] = getCodePathUrls): URL = {
    val codePaths = f(archive)
    val jarPath = codePaths.filter(u => u.getPath.endsWith(".jar")).headOption
    jarPath match {
      case None => throw new Exception(s"Could not find jar file for $archive")
      case _ => jarPath.get
    }
  }

  /**
   * Search the paths to class folder or jar files for the specified archive
   * @param archive archive to search
   * @return Array of URLs to the found class folder and jar files
   */
  def getCodePathUrls(archive: String): Array[URL] = {
    val classDirectory: Path = Directory.Current.get / archive / "target" / "scala-2.10" / "classes"
    val developmentJar: Path = Directory.Current.get / archive / "target" / "scala-2.10" / (archive + ".jar")
    val deployedJar: Path = Directory.Current.get / "lib" / (archive + ".jar")
    val urls = Array(
      Directory(classDirectory).exists.option {
        println(s"Found class directory at $classDirectory")
        classDirectory.toURL
      },
      File(developmentJar).exists.option {
        println(s"Found jar at $developmentJar")
        developmentJar.toURL
      },
      File(deployedJar).exists.option {
        println(s"Found jar at $deployedJar")
        deployedJar.toURL
      }).flatten

    urls
  }

  def buildClassLoader(archive: String, parent: ClassLoader): ClassLoader = {
    //TODO: Allow directory to be passed in, or otherwise abstracted?
    //TODO: Make sensitive to actual scala version rather than hard coding.
    val urls = getCodePathUrls(archive)
    urls match {
      case u if u.length > 0 => new URLClassLoader(u, parent)
      case _ => throw new Exception(s"Could not locate archive $archive")
    }
  }

  lazy val interfaces = buildClassLoader("interfaces", getClass.getClassLoader)

  def usage() = println("Usage: java -jar launcher.jar <archive> <application>")

  if (args.length != 2) {
    usage()
  }
  else {
    try {
      val instance = getArchive(args(0), args(1))
    }
    catch {
      case NonFatal(e) => println(e)
    }
  }
}
