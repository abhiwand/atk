package services

package object authorize {
  object Providers extends Enumeration{
    type Providers = Value
    val None, GooglePlus = Value
  }
}
