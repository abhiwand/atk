package services.authorize.providers.google

import services.authorize.{TokenResponse}

class GoogleTokenResponse(_aa:String, access_token: String, authuser: String, client_id: String)
  extends TokenResponse(_aa, access_token, authuser, client_id){

}

