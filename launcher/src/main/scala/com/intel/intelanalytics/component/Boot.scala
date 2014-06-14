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

import java.net.{URL, URLClassLoader}

import com.typesafe.config.ConfigFactory

import scala.reflect.io.{Directory, File, Path}
import scala.util.control.NonFatal

/**
 * Entry point for all Intel Analytics Toolkit applications.
 *
 * Manages a registry of plugins.
 *
 */
object Boot extends App {

  //Scalaz also provides this, but we don't want a scalaz dependency in the launcher
  implicit class RichBoolean(val b: Boolean) extends AnyVal {
    final def option[A](a: => A): Option[A] = if (b) Some(a) else None
  }

  private var loaders: Map[String, ClassLoader] = Map.empty

  private var archives: Map[String, Component] = Map.empty

  private [intelanalytics] val config = ConfigFactory.load().getConfig("intel.analytics")

  def attempt[T](expr: =>T, failureMessage: =>String) = {
    try {
      expr
    } catch {
      case NonFatal(e) => throw new Exception(failureMessage, e)
    }
  }

  private def buildArchive(archive: String, className: String, configPath: Option[String]): Component = {
    val thread = Thread.currentThread()
    val prior = thread.getContextClassLoader
    val loader = getClassLoader(archive)

    try {
      val klass = attempt(loader.loadClass(className), s"Could not find class $className in archive $archive")
      thread.setContextClassLoader(loader)
      val instance = attempt(klass.newInstance().asInstanceOf[Component],
        s"Found class $className in archive $archive, but it is not a Component")
      val restrictedConfigPath: String = configPath.getOrElse(instance.defaultLocation).replace("/", ".")
      val restrictedConfig = attempt(config.getConfig(restrictedConfigPath),
        s"Could not obtain configuration for class $className in archive $archive with path $restrictedConfigPath")
      attempt(instance.start(restrictedConfig),
          s"Could not start component $className in archive $archive with path $restrictedConfigPath")
      archives += ((archive + ":" + className) -> instance)
      instance
    }
    finally {
      thread.setContextClassLoader(prior)
    }
  }

  /**
   * Returns the requested archive, loading it if needed.
   * @param archive the name of the archive
   * @param className the class to instantiate for communication with the archive. This
   *                  class should implement Component with Locator.
   * @param configPath the subset of the application configuration that should be available
   *                   to this archive. Only relevant if this call causes the archive to be
   *                   built. Empty string means given the component access to whatever
   *                   portion of the config it requests based on its desired position in
   *                   the plugin tree.
   * @return the requested archive
   */
  def getArchive(archive: String, className: String, configPath: Option[String] = None): Component = {
    archives.getOrElse(archive + ":" + className, buildArchive(archive, className, configPath))
  }

  /**
   * Returns the class loader for the given archive
   */
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
    val jarPath = codePaths.find(u => u.getPath.endsWith(".jar"))
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
    //TODO: Allow directory to be passed in, or otherwise abstracted?
    //TODO: Make sensitive to actual scala version rather than hard coding.
    val classDirectory: Path = Directory.Current.get / archive / "target" / "classes"
    val giraphClassDirectory: Path = Directory.Current.get / "igiraph" / archive.substring(1) / "target" / "classes"
    val developmentJar: Path = Directory.Current.get / archive / "target" / (archive + ".jar")
    val giraphJar: Path = Directory.Current.get / "igiraph" / archive.substring(1) / "target" / (archive + ".jar")
    val deployedJar: Path = Directory.Current.get / "lib" / (archive + ".jar")
    val urls = Array(
      Directory(classDirectory).exists.option {
        println(s"Found class directory at $classDirectory")
        classDirectory.toURL
      },
      Directory(giraphClassDirectory).exists.option {
        println(s"Found class directory at $giraphClassDirectory")
        giraphClassDirectory.toURL
      },
      File(developmentJar).exists.option {
        println(s"Found jar at $developmentJar")
        developmentJar.toURL
      },
      File(giraphJar).exists.option {
        println(s"Found jar at $giraphJar")
        giraphJar.toURL
      },
      File(deployedJar).exists.option {
        println(s"Found jar at $deployedJar")
        deployedJar.toURL
      }).flatten

    urls
  }

  private def buildClassLoader(archive: String, parent: ClassLoader): ClassLoader = {
    //TODO: Allow directory to be passed in, or otherwise abstracted?
    //TODO: Make sensitive to actual scala version rather than hard coding.
    val urls = getCodePathUrls(archive)
    val loader = urls match {
      case u if u.length > 0 => new URLClassLoader(u, parent)
      case _ => throw new Exception(s"Could not locate archive $archive")
    }
    loaders += (archive -> loader)
    loader
  }

  private lazy val interfaces = buildClassLoader("interfaces", getClass.getClassLoader)

  def usage() = println("Usage: java -jar launcher.jar <archive> <application>")

  if (args.length != 2) {
    usage()
  }
  else {
    try {
      val instance = getArchive(args(0), args(1))
    }
    catch {
      case NonFatal(e) =>
        println(e)
        e.printStackTrace()
    }
  }
}
