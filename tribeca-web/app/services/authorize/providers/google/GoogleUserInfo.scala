package services.authorize.providers.google

import services.authorize.UserInfo

case class GoogleUserInfo(id:String, email:String, verified_email:Boolean,name:String, given_name:String,
                          family_name:String,link:String,picture:String,gender:String, locale:String)

