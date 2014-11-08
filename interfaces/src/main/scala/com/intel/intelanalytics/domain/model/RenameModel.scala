package com.intel.intelanalytics.domain.model

case class RenameModel(model: ModelReference, newName: String) {
  require(model != null, "model is required")
  require(newName != null && newName.size > 0, "newName is required")
}
