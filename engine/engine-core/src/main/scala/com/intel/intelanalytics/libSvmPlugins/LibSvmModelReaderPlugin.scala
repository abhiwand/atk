package com.intel.intelanalytics.libSvmPlugins

import java.io._

import _root_.libsvm.svm
import com.intel.intelanalytics.interfaces.{ ModelLoader, Model }

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

    libsvmModel.name = "AnjaliModel"
    libsvmModel.asInstanceOf[Model]
  }
}
