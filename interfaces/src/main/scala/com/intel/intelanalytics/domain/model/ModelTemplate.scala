package com.intel.intelanalytics.domain.model

/**
 * Arguments for creating the metadata entry for a model.
 * @param name The user's name for the model.
 */
case class ModelTemplate(name: String, modelType: String) {
  require(name != null, "name must not be null")
  require(name.trim.length > 0, "name must not be empty or whitespace")
  require(modelType != null, "modelType must not be null")
}
