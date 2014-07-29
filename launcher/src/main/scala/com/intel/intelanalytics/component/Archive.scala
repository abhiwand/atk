package com.intel.intelanalytics.component

import scala.reflect.ClassTag

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

trait Archive extends Component {

  private var _loader: Option[String => Any] = None
  private var _definition: Option[ArchiveDefinition] = None

  /**
   * Configuration / definition of the archive, including name and other
   * useful information
   */
  def definition: ArchiveDefinition = _definition.getOrElse(
    throw new Exception("Archive has not been initialized"))

  /**
   * The archive's loader function for loading classes.
   */
  private def loader: String => Any = _loader.getOrElse(
    throw new Exception("Archive has not been initialized")
  )

  /**
   * Called on archive creation to initialize the archive
   */
  private[intelanalytics] def initializeArchive(definition: ArchiveDefinition,
                                                loader: String => Any) = {
    this._loader = Some(loader)
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
    Archive.logger(s"Loading component $path")
    val className = configuration.getString(path.replace("/", ".") + ".class")
    val component = load(className).asInstanceOf[Component]
    val restricted = configuration.getConfig(path + ".config").withFallback(configuration).resolve()
    Archive.logger(s"Component config for $path follows:")
    Archive.logger(restricted.root().render())
    Archive.logger(s"End component config for $path")
    Boot.writeFile("/tmp/iat/" + path.replace("/", "_") + ".effective-conf", restricted.root().render())
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
    Archive.logger(s"Loading class $className")
    loader(className)
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
  var logger: String => Unit = println
}
