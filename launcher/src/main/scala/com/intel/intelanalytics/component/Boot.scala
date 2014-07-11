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

import com.typesafe.config.{ Config, ConfigFactory }

import scala.reflect.io.{ Directory, File, Path }
import scala.util.Try
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

  private var archives: Map[String, Archive] = Map.empty

  private[intelanalytics] val config = ConfigFactory.load().getConfig("intel.analytics")

  def attempt[T](expr: => T, failureMessage: => String) = {
    try {
      expr
    }
    catch {
      case NonFatal(e) => throw new Exception(failureMessage, e)
    }
  }

  private def startComponent(componentInstance: Component, archive: String, component: String, config: Config): Unit = {
    val restrictedConfigPath: String = componentInstance.defaultLocation.replace("/", ".")
    val restrictedConfig = restrictedConfigPath match {
      case null | "" => config
      case p => attempt(config.getConfig(p),
        s"Could not obtain configuration for class ${componentInstance.getClass.getName} " +
          s"in archive $archive with path $restrictedConfigPath")
    }
    componentInstance.init(component, restrictedConfig)
    componentInstance.start()
  }

  private def defaultInit(instance: Any, archive: String, component: String, config: Config) {
    instance match {
      case componentInstance: Component => startComponent(componentInstance, archive, component, config)
      case _ =>
    }
  }

  private def buildArchive(archiveName: String): Archive = {
    //We first create a standard plugin classloader, which we will use to query the config
    //to see if this archive needs special treatment (i.e. a parent class loader other than the
    //interfaces class loader)
    val probe = buildClassLoader(archiveName, interfaces)
    val additionalConfig = ConfigFactory.load(probe)
    val augmented = additionalConfig.withFallback(config)
    val configKey = "intel.analytics.component.archives." + archiveName
    val (parentName, className) = Try {
      val restricted = Try { augmented.getConfig(configKey) }
      val parent = Try {
        restricted.get.getString("parent")
      }.orElse(Try { "interfaces" })
      val className = Try {
        restricted.get.getString("class")
      }.orElse(Try { "com.intel.intelanalytics.component.DefaultArchive" })
      (parent.get, className.get)
    }.getOrElse(("interfaces", "com.intel.intelanalytics.component.DefaultArchive"))

    //Now that we know the parent, we build the real classloader we're going to use for this archive.
    val parentLoader = loaders.getOrElse(parentName, throw new IllegalArgumentException(
      s"Archive $parentName not found when searching for parent archive for ${archiveName}"))

    val loader = buildClassLoader(archiveName, parentLoader)

    //this is the function that will become the basis of the loader function for the archive.
    //the init param is used because we will want to initialize the archive slightly differently
    //than other components, so we need to control the initialization process here.
    def load(name: String, init: Any => Unit) = {
      val thread = Thread.currentThread()
      val prior = thread.getContextClassLoader
      try {
        val klass = attempt(loader.loadClass(name),
          s"Could not find class $name in archive ${archiveName}")
        thread.setContextClassLoader(loader)
        val instance = attempt(klass.newInstance(), s"Could not instantiate class $name")
        init(instance)
        instance
      }
      finally {
        thread.setContextClassLoader(prior)
      }
    }

    val instance = load(className, inst => {
      val archiveInstance = attempt(inst.asInstanceOf[Archive],
        s"Loaded class ${className} in archive ${archiveName}, but it is not an Archive")
      archiveInstance.setLoader((component, name) => load(name, inst => defaultInit(inst, archiveName, component, augmented)))
      defaultInit(archiveInstance, archiveName, archiveName, augmented)
      //cleanup stuff on exit
      Runtime.getRuntime.addShutdownHook(new Thread() {
        override def run(): Unit = {
          archiveInstance.stop()
        }
      })
    })

    val archiveInstance = instance.asInstanceOf[Archive]
    archives += (archiveName -> archiveInstance)
    println(s"Registered archive $archiveName with parent $parentName")
    archiveInstance
  }

  /**
   * Returns the requested archive, loading it if needed.
   * @param name the name of the archive
   *
   * @return the requested archive
   */
  def getArchive(name: String): Archive = {
    archives.getOrElse(name, buildArchive(name))
  }

  /**
   * Returns the class loader for the given archive
   */
  def getClassLoader(archive: String): ClassLoader = {
    loaders.getOrElse(archive, throw new IllegalArgumentException(s"Archive $archive has not been loaded yet"))
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
      val name: String = args(0)
      println(s"Starting $name")
      val instance = getArchive(name)
      println(s"Started $name")
    }
    catch {
      case NonFatal(e) =>
        println(e)
        e.printStackTrace()
    }
  }
}
