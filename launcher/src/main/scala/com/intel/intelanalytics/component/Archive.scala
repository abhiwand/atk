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

import java.net.URL

import com.intel.intelanalytics.component.Boot._
import com.typesafe.config.{ ConfigParseOptions, ConfigFactory, Config }

import scala.reflect.ClassTag
import scala.reflect.io.{ File, Directory, Path }

trait Archive extends Component with ClassLoaderAware {

  private var _classLoader: Option[ClassLoader] = None
  private var _definition: Option[ArchiveDefinition] = None

  /**
   * The class loader associated with the archive
   */
  def classLoader: ClassLoader = attempt(_classLoader.get, "Archive has not been initialized")

  /**
   * Execute a code block using the ClassLoader defined in the {classLoader} member
   * rather than the class loader that loaded this class or the current thread class loader.
   */
  override def withMyClassLoader[T](expression: => T): T = withLoader(classLoader)(expression)

  /**
   * Configuration / definition of the archive, including name and other
   * useful information
   */
  def definition: ArchiveDefinition = attempt(_definition.get, "Archive has not been initialized")

  /**
   * Called on archive creation to initialize the archive
   */
  private[intelanalytics] def initializeArchive(definition: ArchiveDefinition,
                                                configuration: Config,
                                                classLoader: ClassLoader) = {
    init(definition.name, configuration)
    this._classLoader = Some(classLoader)
    this._definition = Some(definition)
  }

  /**
   * Load and initialize a `Component` based on configuration path.
   *
   * @param path the config path to look up. The path should contain a "class" entry
   *             that holds the string name of the class that should be instantiated,
   *             which should be a class that's visible from this archive's class loader.
   * @return an initialized and started instance of the component
   */
  protected def loadComponent(path: String): Component = {
    Archive.logger(s"Loading component $path (set ${SystemConfig.debugConfigKey} to true to enable component config logging")
    val className = configuration.getString(path.replace("/", ".") + ".class")
    val component = load(className).asInstanceOf[Component]
    val restricted = configuration.getConfig(path + ".config").withFallback(configuration).resolve()
    if (Archive.system.debugConfig) {
      Archive.logger(s"Component config for $path follows:")
      Archive.logger(restricted.root().render())
      Archive.logger(s"End component config for $path")
      writeFile(Archive.TMP + path.replace("/", "_") + ".effective-conf", restricted.root().render())
    }
    component.init(path, restricted)
    component.start()
    component
  }

  /**
   * Called by archives in order to load new instances from the archive. Does not provide
   * any caching of instances.
   *
   * @param className the class name to instantiate and configure
   * @return the new instance
   */
  def load(className: String): Any = {
    Archive.logger(s"Archive ${definition.name} creating instance of class $className")
    withMyClassLoader {
      classLoader.loadClass(className).newInstance()
    }
  }

  /**
   * Obtain instances of a given class. The keys are established purely
   * by convention.
   *
   * @param descriptor the string key of the desired class instance.
   * @tparam T the type of the requested instances
   * @return the requested instances, or the empty sequence if no such instances could be produced.
   */
  def getAll[T: ClassTag](descriptor: String): Seq[T]

  /**
   * Obtain a single instance of a given class. The keys are established purely
   * by convention.
   *
   * @param descriptor the string key of the desired class instance.
   * @tparam T the type of the requested instances
   * @return the requested instance, or the first such instance if the locator provides more than one
   * @throws NoSuchElementException if no instances were found
   */
  def get[T: ClassTag](descriptor: String): T = getAll(descriptor).headOption
    .getOrElse(throw new NoSuchElementException(
      s"No class matching descriptor $descriptor was found in location '${definition.name}'"))

}

/**
 * Companion object for Archives.
 */
object Archive {

  /**
   * Can be set at runtime to use whatever logging framework is desired.
   */
  var logger: String => Unit = println

  /**
   * Returns the requested archive, loading it if needed.
   * @param archiveName the name of the archive
   * @param className the name of the class managing the archive
   *
   * @return the requested archive
   */
  def getArchive(archiveName: String, className: Option[String] = None): Archive = {
    system.archive(archiveName).getOrElse(buildArchive(archiveName, className))
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
  private[component] def buildClassLoader(archive: String, parent: ClassLoader): ClassLoader = {
    val urls = Archive.getCodePathUrls(archive)
    val loader = urls match {
      case u if u.length > 0 => new ArchiveClassLoader(archive, u, parent)
      case _ => throw new Exception(s"Could not locate archive $archive")
    }
    loader
  }

  private val defaultParentArchiveName = System.getenv("IAT_DEFAULT_ARCHIVE_PARENT") match {
    case null => "interfaces"
    case s if s.trim == "" => "interfaces"
    case parent => parent
  }

  private [component] val TMP = "/tmp/iat-" + java.util.UUID.randomUUID.toString + "/"

  //Initialize system with Config that includes only normal class loader. This should be enough for
  //anything that's supposed to be a default parent archive.
  var _system = new SystemConfig(ConfigFactory.load())

  private val defaultParentArchiveClassLoader = getArchive(defaultParentArchiveName).classLoader

  //Now that we know the default parent archive, reload configs for the system with that in mind.
  _system = _system.withRootConfiguration(ConfigFactory.load(defaultParentArchiveClassLoader))

  def system: SystemConfig = _system


  /**
   * Initializes an archive instance
   *
   * @param definition the definition (name, etc.)
   * @param classLoader  a class loader for the archive
   * @param augmentedConfig config that is specific to this archive
   * @param instance the (un-initialized) archive instance
   */
  private def initializeArchive(definition: ArchiveDefinition,
                                classLoader: ClassLoader,
                                augmentedConfig: Config,
                                instance: Archive) = {

    instance.initializeArchive(definition, augmentedConfig, classLoader)

    //Give each Archive a chance to clean up when the app shuts down
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        instance.stop()
      }
    })
  }

  /**
   * Main entry point for archive creation
   *
   * @param archiveName the archive to create
   * @param inProgress The set of archive names that are already being created, used to detect circular parent chains
   * @return the created, running, `Archive`
   */
  private def buildArchive(archiveName: String,
                           className: Option[String] = None,
                           inProgress: List[String] = List.empty): Archive = {
    require(!inProgress.contains(archiveName),
      "Circular parent relationship among archives: " + (archiveName :: inProgress).mkString(","))
    try {
      //We first create a standard plugin class loader, which we will use to query the config
      //to see if this archive needs special treatment (i.e. a parent class loader other than the
      //defaultParentArchive class loader)
      val probe = Archive.buildClassLoader(archiveName, defaultParentArchiveClassLoader)

      val parseOptions = ConfigParseOptions.defaults()
      parseOptions.setAllowMissing(true)

      val augmentedConfigForProbe = ConfigFactory.defaultReference(probe)

      val definition = {
        val defaultDef = ArchiveDefinition(archiveName, augmentedConfigForProbe, defaultParentArchiveName)
        className match {
          case Some(n) => defaultDef.copy(className = n)
          case _ => defaultDef
        }
      }

      //Now that we know the parent, we build the real class loader we're going to use for this archive.
      val parentLoader = system.loader(definition.parent).getOrElse {
        if (definition.parent == definition.name) {
          Archive.getClass.getClassLoader
        }
        else {
          buildArchive(definition.parent, inProgress = definition.name :: inProgress).classLoader
        }
      }

      val loader = Archive.buildClassLoader(archiveName, parentLoader)

      ConfigFactory.invalidateCaches()

      val augmentedConfig = system.rootConfiguration.withFallback(
        ConfigFactory.parseResources(loader, "reference.conf", parseOptions)
          .withFallback(system.rootConfiguration)).resolve()

      if (system.debugConfig)
        writeFile(TMP + archiveName + ".effective-conf", augmentedConfig.root().render())

      val instance = attempt(loader.loadClass(definition.className).newInstance(),
        s"Loaded class ${definition.className} in archive ${definition.name}, but could not create an instance of it")

      val archiveInstance = attempt(instance.asInstanceOf[Archive],
        s"Loaded class ${definition.className} in archive ${definition.name}, but it is not an Archive")

      withLoader(loader) {
        initializeArchive(definition, loader, augmentedConfig.getConfig(definition.configPath), archiveInstance)
        val currentSystem = system
        try {
          synchronized {
            _system = system.addArchive(archiveInstance)
          }
          Archive.logger(s"Registered archive $archiveName with parent ${definition.parent}")
          archiveInstance.start()
        }
        catch {
          case e: Exception => synchronized {
            _system = currentSystem
            throw e
          }
        }
      }

      archiveInstance
    }
    catch {
      case e: Exception => throw new ArchiveInitException("Exception while building archive: " + archiveName, e)
    }
  }

}
