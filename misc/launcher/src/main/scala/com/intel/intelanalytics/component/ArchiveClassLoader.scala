/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

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
