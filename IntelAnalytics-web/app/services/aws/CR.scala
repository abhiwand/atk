package services.aws

class CR(config: ConfigObject) {
  val origin = config.toConfig.getString("origin")
  val methods = setMethods(config.toConfig.getStringList("methods").toList)
  val allowedHeaders = config.toConfig.getStringList("allowedHeaders").toList
  val exposedHeaders = config.toConfig.getStringList("exposedHeaders").toList

  def setMethods(methods: List[String]): List[CORSRule.AllowedMethods] = {
    var allowedMethods: List[CORSRule.AllowedMethods] = List[CORSRule.AllowedMethods]()
    for (method <- methods) {
      allowedMethods ::= CORSRule.AllowedMethods.fromValue(method)
    }
    allowedMethods
  }
}
