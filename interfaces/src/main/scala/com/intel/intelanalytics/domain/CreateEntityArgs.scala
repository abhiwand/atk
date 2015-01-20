package com.intel.intelanalytics.domain

/**
 * Arguments needed to create a new entity
 *
 * @param name - e.g. for the entity type "model:logistic_regression", the subtype is "logistic_regression"
 * @param entityType - e.g. for the entity type "model:logistic_regression", the subtype is "logistic_regression"
 * @param description - optional description of the particular entity or why it was created
 */
case class CreateEntityArgs(name: Option[String] = None, entityType: Option[String] = None, description: Option[String] = None)

// consciously not adding a name parameter here to avoid coupling naming with creation.  Make naming an explicit command
