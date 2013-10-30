package models

object Whitelists {
  def anonymousWhitelist(): database.WhiteList = {
    database.WhiteList(Some(0),"")
  }
}
