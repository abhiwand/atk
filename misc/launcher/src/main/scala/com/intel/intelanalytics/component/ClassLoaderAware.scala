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

trait ClassLoaderAware {

  /**
   * Execute a code block using specified class loader
   * rather than the ClassLoader of the currentThread()
   */
  def withLoader[T](loader: ClassLoader)(expr: => T): T = {
    val prior = Thread.currentThread().getContextClassLoader
    try {
      Thread.currentThread().setContextClassLoader(loader)
      expr
    }
    finally {
      Thread.currentThread().setContextClassLoader(prior)
    }
  }

  /**
   * Execute a code block using the ClassLoader of 'this'
   * rather than the ClassLoader of the currentThread()
   */
  def withMyClassLoader[T](expression: => T): T = {
    val prior = Thread.currentThread().getContextClassLoader
    try {
      val loader = this.getClass.getClassLoader
      Thread.currentThread setContextClassLoader loader
      expression
    }
    finally {
      Thread.currentThread setContextClassLoader prior
    }
  }
}
