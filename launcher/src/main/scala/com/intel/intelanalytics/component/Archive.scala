//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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

import com.typesafe.config.{ ConfigParseOptions, ConfigFactory, Config }

import scala.reflect.ClassTag
import scala.reflect.io.{ File, Directory, Path }
import scala.util.Try

import ExceptionUtil.attempt

abstract class Archive(val definition: ArchiveDefinition, val classLoader: ClassLoader, config: Config)
    extends Component with ClassLoaderAware {

  //Component initialization
  init(definition.name, config)

  /**
   * Execute a code block using the `ClassLoader` defined in the {classLoader} member
   * rather than the class loader that loaded this class or the current thread class loader.
   */
  override def withMyClassLoader[T](expression: => T): T = withLoader(classLoader)(expression)

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
    if (Archive.system.systemConfig.debugConfig) {
      Archive.logger(s"Component config for $path follows:")
      Archive.logger(restricted.root().render())
      Archive.logger(s"End component config for $path")
      FileUtil.writeFile(Archive.system.systemConfig.debugConfigFolder + path.replace("/", "_") + ".effective-conf",
        restricted.root().render())
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
object Archive extends ClassLoaderAware {

  /**
   * Can be set at runtime to use whatever logging framework is desired.
   */
  //TODO: load slf4j + logback in separate class loader (completely separate from this one) and use it.
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
  def getJar(archive: String, f: String => Array[URL] = a => getCodePathUrls(a)): URL = {
    val codePaths = f(archive)
    val jarPath = codePaths.find(u => u.getPath.endsWith(".jar"))
    jarPath match {
      case None => throw new Exception(s"Could not find jar file for $archive")
      case _ => jarPath.get
    }
  }

  /**
   * Locations from which classes can be loaded.
   */
  sealed trait CodePath { def name: String; def path: Path }
  case class JarPath(name: String, path: Path) extends CodePath
  case class FolderPath(name: String, path: Path) extends CodePath

  /**
   * Search the paths to class folder or jar files for the specified archive
   * @param archive archive to search
   * @return Array of CodePaths to the found class folder and jar files
   */
  def getCodePaths(archive: String, sourceRoots: Array[String], jarFolders: Array[String]): Array[CodePath] = {
    //TODO: Sometimes ${PWD} doesn't get replaced by Config library, figure out why
    val paths = sourceRoots.map(s => s.replace("${PWD}", Directory.Current.get.toString()): Path).flatMap { root =>
      val baseSearchPath = Array(
        FolderPath("development class files", root / archive / "target" / "classes"),
        FolderPath("development resource files", root / archive / "src" / "main" / "resources"),
        JarPath("development jar", root / archive / "target" / (archive + ".jar")),
        FolderPath("giraph development class files", root / "giraph-plugins" / archive.substring(1) / "target" / "classes"),
        FolderPath("giraph development resource files", root / "giraph-plugins" / archive.substring(1) / "src" / "main" / "resources"),
        JarPath("giraph development jar", root / "giraph-plugins" / archive.substring(1) / "target" / (archive + ".jar")),
        JarPath("yarn cluster mode", root / (archive + ".jar")), /* In yarn container mode, all jars are copied to root */
        JarPath("launcher", root / ".." / (archive + ".jar"))
      )
      archive match {
        case "engine" => baseSearchPath :+ JarPath("__spark__", root / "__spark__.jar")
        case _ => baseSearchPath
      }
    } ++ jarFolders.map(s => JarPath("deployed jar",
      (s.replace("${PWD}", Directory.Current.get.toString()): Path) / (archive + ".jar")))
    paths.foreach { p =>
      Archive.logger(s"Considering ${p.path}")
    }
    paths.filter {
      case JarPath(n, p) => File(p).exists
      case FolderPath(n, p) => Directory(p).exists
    }.toArray
  }

  /**
   * Search the paths to class folder or jar files for the specified archive
   * @param archive archive to search
   * @return Array of URLs to the found class folder and jar files
   */
  def getCodePathUrls(archive: String,
                      sourceRoots: Array[String] = system.systemConfig.sourceRoots,
                      jarFolders: Array[String] = system.systemConfig.jarFolders): Array[URL] = {
    getCodePaths(archive, sourceRoots, jarFolders).map { codePath =>
      Archive.logger(s"Found ${codePath.name} at ${codePath.path}")
      codePath.path.toURL
    }
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
  private[component] def buildClassLoader(archive: String,
                                          parent: ClassLoader,
                                          sourceRoots: Array[String],
                                          jarFolders: Array[String],
                                          additionalArchives: Array[Archive] = Array.empty[Archive],
                                          additionalPaths: Array[String] = Array.empty[String]): ClassLoader = {
    val urls = Archive.getCodePathUrls(archive, sourceRoots, jarFolders) ++
      additionalPaths.map(normalizeUrl)
    require(urls.length > 0, s"Could not locate archive $archive")
    val loader = new ArchiveClassLoader(archive, urls, parent, additionalArchives.map(_.classLoader))
    Archive.logger(s"Created class loader: $loader")
    loader
  }

  /**
   * Needed to make sure URLs are presented in a form that URLClassLoader can use (e.g. for files, they
   * need to start with file:// and, if they are directories, they need to end with a slash)
   */
  private def normalizeUrl(url: String): URL = {
    val lead = if (url.contains(":")) { "" } else { "file://" }
    val tail = if ((lead + url).startsWith("file:") && !url.endsWith("/") && !url.endsWith(".jar")) {
      "/"
    }
    else { "" }
    new URL(lead + url + tail)
  }

  var _system: SystemState = null

  def system: SystemState = {
    if (_system == null) {
      //Bootstrap process
      //First, construct a bare-bones configuration based on nothing but commandline and the launcher
      //We need this to know basic configs needed to load the first archive
      val tempSystem = new SystemState()
      val defaultParentArchiveName = tempSystem.systemConfig.defaultParentArchiveName
      //Now we can construct a throwaway class loader we use to load the real configuration
      val configDirectory = System.getProperty("launcher.configDirectory", (Directory.Current.get / "conf").toString)
      val defaultParentArchiveClassLoader =
        attempt(buildClassLoader(defaultParentArchiveName,
          getClass.getClassLoader,
          tempSystem.systemConfig.sourceRoots,
          tempSystem.systemConfig.jarFolders,
          Array.empty,
          Array(configDirectory)),
          s"Failed to build default parent class loader '$defaultParentArchiveName'")
      //Install this system configuration that's really close to the final config
      _system = attempt(new SystemState(
        new SystemConfig(ConfigFactory.load(defaultParentArchiveClassLoader))),
        s"Failed to load default configuration")
      //Now we've got enough to actually load our first archive
      //getArchive has the side effect of updating _system also, but it needs _system to work
      buildArchive(_system.systemConfig.defaultParentArchiveName)
      //At this point, the system config is fully initialized and new archives can be loaded.
      logger(s"System configuration installed")
    }
    _system
  }

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

    instance.init(definition.name, augmentedConfig)

    //Give each Archive a chance to clean up when the app shuts down
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        instance.stop()
      }
    })
  }

  def getAugmentedConfig(archiveName: String, loader: ClassLoader) = {
    val parseOptions = ConfigParseOptions.defaults()
    parseOptions.setAllowMissing(true)

    ConfigFactory.invalidateCaches()

    val augmentedConfig = system.systemConfig.rootConfiguration.withFallback(
      ConfigFactory.parseResources(loader, "reference.conf", parseOptions)
        .withFallback(system.systemConfig.rootConfiguration)).resolve()

    if (system.systemConfig.debugConfig)
      FileUtil.writeFile(system.systemConfig.debugConfigFolder + archiveName + ".effective-conf",
        augmentedConfig.root().render())

    augmentedConfig
  }

  /**
   * Main entry point for archive creation
   *
   * @param archiveName the archive to create
   * @return the created, running, `Archive`
   */
  private def buildArchive(archiveName: String,
                           className: Option[String] = None): Archive = {
    try {
      //We first create a standard plugin class loader, which we will use to query the config
      //to see if this archive needs special treatment (i.e. a parent class loader other than the
      //defaultParentArchive class loader)
      val probe = Archive.buildClassLoader(archiveName,
        system.archive(system.systemConfig.defaultParentArchiveName)
          .map(a => a.classLoader)
          .getOrElse(this.getClass.getClassLoader),
        system.systemConfig.sourceRoots,
        system.systemConfig.jarFolders
      )

      val augmentedConfigForProbe = ConfigFactory.defaultReference(probe)

      val definition = {
        val defaultDef = ArchiveDefinition(archiveName, augmentedConfigForProbe, system.systemConfig.defaultParentArchiveName)
        className match {
          case Some(n) => defaultDef.copy(className = n)
          case _ => defaultDef
        }
      }

      //Now that we know the real parent, obtain its class loader.
      val parentLoader = system.loader(definition.parent).getOrElse {
        if (definition.parent == definition.name) {
          Archive.getClass.getClassLoader
        }
        else {
          getArchive(definition.parent).classLoader
        }
      }

      val probeConfig = new SystemConfig(augmentedConfigForProbe)

      val additionalArchiveDependencies = probeConfig.extraArchives(definition.configPath).map(s => getArchive(s))

      //And now we can build the class loader for this archive
      val loader = Archive.buildClassLoader(archiveName,
        parentLoader,
        system.systemConfig.sourceRoots,
        system.systemConfig.jarFolders,
        additionalArchiveDependencies,
        probeConfig.extraClassPath(definition.configPath))

      val augmentedConfig = getAugmentedConfig(archiveName, loader)

      val archiveClass = attempt(loader.loadClass(definition.className),
        s"Archive class ${definition.className} not found")

      val constructor = attempt(archiveClass.getConstructor(classOf[ArchiveDefinition],
        classOf[ClassLoader],
        classOf[Config]),
        s"Class ${definition.className} does not have a constructor of the form (ArchiveDefinition, ClassLoader, Config)")
      val instance = attempt(constructor.newInstance(definition, loader, augmentedConfig),
        s"Loaded class ${definition.className} in archive ${definition.name}, but could not create an instance of it")

      val archiveInstance = attempt(instance.asInstanceOf[Archive],
        s"Loaded class ${definition.className} in archive ${definition.name}, but it is not an Archive")

      val restrictedConfig = Try { augmentedConfig.getConfig(definition.configPath) }.getOrElse(ConfigFactory.empty())

      withLoader(loader) {
        initializeArchive(definition, loader, restrictedConfig, archiveInstance)
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
      case e: Throwable => throw new ArchiveInitException("Exception while building archive: " + archiveName, e)
    }
  }

}
