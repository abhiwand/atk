package services.authorize.providers.google

import services.authorize.UserInfo

case class GoogleUserInfo(id:String, email:String,name:String, given_name:String,
                          family_name:String)

