package services.authorize.providers.google

import services.authorize.{TokenResponse}

class GoogleTokenResponse(access_token: String, authuser: String, client_id: String)
  extends TokenResponse(access_token, authuser, client_id){

}

