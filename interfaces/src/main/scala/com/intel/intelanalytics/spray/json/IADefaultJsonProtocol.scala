package com.intel.intelanalytics.spray.json

import spray.json._

/**
 * Our JsonProtocol is similar to Spray's DefaultJsonProtocol
 * except we handle ProductFormats differently.
 */
trait IADefaultJsonProtocol extends BasicFormats
  with StandardFormats
  with CollectionFormats
  with CustomProductFormats
  with AdditionalFormats

/**
 * Our JsonProtocol is similar to Spray's DefaultJsonProtocol
 * except we handle ProductFormats differently.
 */
object IADefaultJsonProtocol extends IADefaultJsonProtocol

