/**
 *  Copyright (c) 2015 Intel Corporation 
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.trustedanalytics.atk.scoring.models

import java.io._

import org.trustedanalytics.atk.scoring.interfaces.{ Model, ModelLoader }
import libsvm.svm

class LibSvmModelReaderPlugin extends ModelLoader {

  private var libsvmModel: LibSvmModel = _

  override def load(bytes: Array[Byte]): Model = {
    var inputStream: InputStream = null
    var svm_model: Model = null
    try {
      inputStream = new ByteArrayInputStream(bytes)
      val bfReader = new BufferedReader(new InputStreamReader(inputStream))
      libsvmModel = new LibSvmModel(svm.svm_load_model(bfReader))
    }
    catch {
      //TODO: log the error
      case e: IOException => throw e
    }
    finally {
      if (inputStream != null)
        inputStream.close()
    }

    libsvmModel.asInstanceOf[Model]
  }
}
