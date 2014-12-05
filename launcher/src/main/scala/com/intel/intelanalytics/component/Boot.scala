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

import com.typesafe.config.{ ConfigParseOptions, Config, ConfigFactory }

import scala.reflect.io.{ Directory, File, Path }
import scala.util.Try
import scala.util.Random
import scala.util.control.NonFatal

/**
 * Entry point for all Intel Analytics Toolkit applications.
 *
 * Manages a registry of plugins.
 *
 */
object Boot extends App with ClassLoaderAware {

  //Scalaz also provides this, but we don't want a scalaz dependency in the launcher
  implicit class RichBoolean(val b: Boolean) extends AnyVal {
    final def option[A](a: => A): Option[A] = if (b) Some(a) else None
  }

  private var loaders: Map[String, ClassLoader] = Map.empty

  private var archives: Map[String, Archive] = Map.empty

  /**
   * This one is crucial to build first
   */
  private val interfaces = buildClassLoader("interfaces", getClass.getClassLoader)

  private val config: Config = ConfigFactory.load(interfaces)

  val TMP = "/tmp/iat-" + java.util.UUID.randomUUID.toString + "/"

  def attempt[T](expr: => T, failureMessage: => String) = {
    try {
      expr
    }
    catch {
      case NonFatal(e) => throw new Exception(failureMessage, e)
    }
  }

  private def readArchiveDefinition(archiveName: String, config: Config) = {
    val configKey = "intel.analytics.component.archives." + archiveName
    val restricted = Try {
      config.getConfig(configKey)
    }.getOrElse(
      {
        Archive.logger("No config found, using empty")
        ConfigFactory.empty()
      })
    val parent = Try {
      restricted.getString("parent")
    }.getOrElse({
      Archive.logger("Using default value for archive parent")
      "interfaces"
    })
    val className = Try {
      restricted.getString("class")
    }.getOrElse({
      Archive.logger("No class entry found, using default archive class")
      "com.intel.intelanalytics.component.DefaultArchive"
    })

    val configPath = Try {
      restricted.getString("config-path")
    }.getOrElse(
      {
        Archive.logger("No config-path found, using default")
        configKey
      })
    ArchiveDefinition(archiveName, parent, className, configPath)
  }

  /**
   * Packages a class loader with some additional error handling / logging information
   * that's useful for Archives.
   */
  case class ArchiveClassLoader(archiveName: String, loader: ClassLoader) extends (String => Any) {
    override def apply(className: String): Any = {
      val klass = attempt(loader.loadClass(className),
        s"Could not find class $className in archive $archiveName")
      withLoader(loader) {
        val instance = attempt(klass.newInstance(), s"Could not instantiate class $className")
        instance
      }
    }
  }

  /**
   * Initializes an archive instance
   *
   * @param definition the definition (name, etc.)
   * @param boundLoad  a class loading / instantiating function
   * @param augmentedConfig config that is specific to this archive
   * @param instance the (un-initialized) archive instance
   */
  private def initializeArchive(definition: ArchiveDefinition,
                                boundLoad: String => Any,
                                augmentedConfig: Config,
                                instance: Archive) = {

    instance.init(definition.name, augmentedConfig)

    instance.initializeArchive(definition, boundLoad)

    //cleanup stuff on exit
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        instance.stop()
      }
    })
  }

  /**
   * For debugging only
   */
  def writeFile(fileName: String, content: String) {
    import java.io._
    val file = new java.io.File(fileName)
    val parent = file.getParentFile
    if (!parent.exists()) {
      parent.mkdirs()
    }
    val writer = new PrintWriter(file)
    try {
      writer.append(content)
    }
    finally {
      writer.close()
    }
  }

  /**
   * Main entry point for archive creation
   *
   * @param archiveName the archive to create
   * @return the created, running, `Archive`
   */
  private def buildArchive(archiveName: String, className: Option[String] = None): Archive = {
    try {
      //We first create a standard plugin class loader, which we will use to query the config
      //to see if this archive needs special treatment (i.e. a parent class loader other than the
      //interfaces class loader)
      val probe = buildClassLoader(archiveName, interfaces)

      val parseOptions = ConfigParseOptions.defaults()
      parseOptions.setAllowMissing(true)

      val augmentedConfigForProbe = ConfigFactory.defaultReference(probe)

      val definition = {
        val defaultDef = readArchiveDefinition(archiveName, augmentedConfigForProbe)
        className match {
          case Some(n) => defaultDef.copy(className = n)
          case _ => defaultDef
        }
      }

      //Now that we know the parent, we build the real class loader we're going to use for this archive.
      val parentLoader = loaders.getOrElse(definition.parent, throw new IllegalArgumentException(
        s"Archive ${definition.parent} not found when searching for parent archive for $archiveName"))

      val loader = buildClassLoader(archiveName, parentLoader)

      ConfigFactory.invalidateCaches()

      val augmentedConfig = config.withFallback(
        ConfigFactory.parseResources(loader, "reference.conf", parseOptions).withFallback(config)).resolve()

      writeFile(TMP + archiveName + ".effective-conf", augmentedConfig.root().render())

      val archiveLoader = ArchiveClassLoader(archiveName, loader)

      val instance = archiveLoader(definition.className)

      val archiveInstance = attempt(instance.asInstanceOf[Archive],
        s"Loaded class ${instance.getClass.getName} in archive ${definition.name}, but it is not an Archive")

      withLoader(loader) {
        initializeArchive(definition, archiveLoader, augmentedConfig.getConfig(definition.configPath), archiveInstance)
        archives += (archiveName -> archiveInstance)
        Archive.logger(s"Registered archive $archiveName with parent ${definition.parent}")
        archiveInstance.start()
      }

      archiveInstance
    }
    catch {
      case e: Exception => throw new ArchiveInitException("building archive: " + archiveName, e)
    }
  }

  /**
   * Returns the requested archive, loading it if needed.
   * @param archiveName the name of the archive
   * @param className the name of the class managing the archive
   *
   * @return the requested archive
   */
  def getArchive(archiveName: String, className: Option[String] = None): Archive = {
    archives.getOrElse(archiveName, buildArchive(archiveName, className))
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
    // Development environment support - loose class files, source resources, jars where initially built
    val classDirectory: Path = Directory.Current.get / archive / "target" / "classes"
    val sourceResourceDirectory: Path = Directory.Current.get / archive / "src" / "main" / "resources"
    val developmentJar: Path = Directory.Current.get / archive / "target" / (archive + ".jar")

    //Special case for igiraph since it follows a non-standard folder layout
    val giraphClassDirectory: Path = Directory.Current.get / "igiraph" / archive.substring(1) / "target" / "classes"
    val giraphSourceResourceDirectory: Path =
      Directory.Current.get / "igiraph" / archive.substring(1) / "src" / "main" / "resources"
    val giraphJar: Path = Directory.Current.get / "igiraph" / archive.substring(1) / "target" / (archive + ".jar")

    // Deployed environment - all jars in lib folder
    val deployedJar: Path = Directory.Current.get / "lib" / (archive + ".jar")

    val urls = Array(
      Directory(sourceResourceDirectory).exists.option {
        Archive.logger(s"Found source resource directory at $sourceResourceDirectory")
        sourceResourceDirectory.toURL
      },
      Directory(giraphSourceResourceDirectory).exists.option {
        Archive.logger(s"Found source resource directory at $giraphSourceResourceDirectory")
        giraphSourceResourceDirectory.toURL
      },
      Directory(classDirectory).exists.option {
        Archive.logger(s"Found class directory at $classDirectory")
        classDirectory.toURL
      },
      Directory(giraphClassDirectory).exists.option {
        Archive.logger(s"Found class directory at $giraphClassDirectory")
        giraphClassDirectory.toURL
      },
      File(developmentJar).exists.option {
        Archive.logger(s"Found jar at $developmentJar")
        developmentJar.toURL
      },
      File(giraphJar).exists.option {
        Archive.logger(s"Found jar at $giraphJar")
        giraphJar.toURL
      },
      File(deployedJar).exists.option {
        Archive.logger(s"Found jar at $deployedJar")
        deployedJar.toURL
      }).flatten

    urls
  }

  /**
   * Create a class loader for the given archive, with the given parent.
   *
   * As a side effect, updates the loaders map.
   *
   * @param archive the archive whose class loader we're constructing
   * @param parent the parent for the new class loader
   * @return a class loader
   */
  private def buildClassLoader(archive: String, parent: ClassLoader): ClassLoader = {
    val urls = getCodePathUrls(archive)
    val loader = urls match {
      case u if u.length > 0 => new URLClassLoader(u, parent)
      case _ => throw new Exception(s"Could not locate archive $archive")
    }
    synchronized {
      loaders += (archive -> loader)
    }
    loader
  }

  def usage() = println("Usage: java -jar launcher.jar <archive> <application>")

  if (args.length < 1 || args.length > 2) {
    usage()
  }
  else {
    try {
      val archiveName: String = args(0)
      val applicationName: Option[String] = { if (args.length == 2) Some(args(1)) else None }
      Archive.logger(s"Starting $archiveName")
      val instance = getArchive(archiveName, applicationName)
      Archive.logger(s"Started $archiveName with ${instance.definition}")
    }
    catch {
      case NonFatal(e) =>
        Archive.logger(e.toString)
        println(e)
        e.printStackTrace()
    }
  }
}
