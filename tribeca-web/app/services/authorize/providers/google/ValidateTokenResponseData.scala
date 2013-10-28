package services.authorize.providers.google

case class ValidateTokenResponseData(_aa: String, access_token: String, authuser: String, client_id: String, code: String,
                                      cookie_policy: String, expires_at: String, expires_in: String, g_user_cookie_policy:String,
                                      id_token: String, issued_at: String,prompt: String, response_type: String, scope: String,
                                      session_state: String, state: String, token_type: String)
