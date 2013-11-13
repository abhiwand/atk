package services.aws

import com.typesafe.config.{ConfigObject}
import scala.collection.JavaConversions._
import com.amazonaws.services.s3.model.CORSRule

class CR(config: ConfigObject) {
  val origin = config.toConfig.getString("origin")
  val methods = setMethods(config.toConfig.getStringList("methods").toList)
  val allowedHeaders = config.toConfig.getStringList("allowedHeaders").toList
  val exposedHeaders = config.toConfig.getStringList("exposedHeaders").toList

  def setMethods(methods: List[String]): List[CORSRule.AllowedMethods] = {
    var allowedMethods: List[CORSRule.AllowedMethods] = List[CORSRule.AllowedMethods]()
    for( method <- methods){
      allowedMethods ::= CORSRule.AllowedMethods.fromValue(method)
    }
    allowedMethods
  }
}
