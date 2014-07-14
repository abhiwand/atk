package com.intel.intelanalytics.component

import com.intel.intelanalytics.component.Boot.ArchiveDefinition

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

  def definition: ArchiveDefinition = _definition.getOrElse(
    throw new Exception("Archive has not been initialized"))

  private def loader: String => Any = _loader.getOrElse(
    throw new Exception("Archive has not been initialized")
  )

  private[intelanalytics] def initializeArchive(definition: ArchiveDefinition,
                                                loader: String => Any) = {
    this._loader = Some(loader)
    this._definition = Some(definition)
  }

  protected def loadComponent(path: String): Component = {
    Archive.logger(s"Loading component $path")
    val className = configuration.getString(path.replace("/", ".") + ".class")
    val component = load(className).asInstanceOf[Component]
    val restricted = configuration.getConfig(path + ".config")
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
  protected def load(className: String): Any = {
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

object Archive {
  private var _logger: Option[String => Unit] = Some(println)

  /**
   * A function that the archive can use to log debug information.
   */
  def logger_=(function: String => Unit): Unit = {
    _logger = Some(function)
  }

  /**
   * A function that the archive can use to log debug information.
   */
  def logger = _logger.getOrElse(throw new Exception("Archive logger not initialized"))

}
