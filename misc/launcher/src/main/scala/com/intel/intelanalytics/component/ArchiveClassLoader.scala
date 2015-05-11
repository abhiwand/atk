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

import java.net.{ URLClassLoader, URL }

import scala.collection.mutable.ArrayBuffer

/**
 * Packages a class loader with some additional error handling / logging information
 * that's useful for Archives.
 *
 * Loads classes first from its parent, then from the URLs provided, then from its dependencies, in order
 */
class ArchiveClassLoader(archiveName: String, urls: Array[URL], parent: ClassLoader, dependencies: Array[ClassLoader])
    extends URLClassLoader(urls, parent) {
  override def loadClass(className: String, resolve: Boolean): Class[_] = {
    //Interestingly, cannot use "attempt" here, have to use try/catch, probably due to stack depth check in ClassLoader.
    try {
      val klass = super.loadClass(className, resolve)
      klass
    }
    catch {
      case e: ClassNotFoundException =>
        for (loader <- dependencies) {
          try {
            return loader.loadClass(className)
          }
          catch {
            case e: Throwable => () //ignore
          }
        }
        throw new ClassNotFoundException(s"Could not find class $className in archive $archiveName", e)
    }
  }

  /**
   * Returns the complete class loader parent chain, starting with this class loader.
   */
  def chain: Array[ClassLoader] = {
    val buff = ArrayBuffer[ClassLoader]()
    var current: ClassLoader = this
    while (current != null) {
      buff.append(current)
      current = current.getParent
    }
    buff.toArray
  }

  override def toString = {
    s"ArchiveClassLoader($archiveName, [${urls.mkString(", ")}], $parent, [${dependencies.mkString(", ")}])"
  }
}
