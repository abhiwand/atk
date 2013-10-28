package services.authorize.providers.google


case class ValidateTokenJson(issued_to: String, audience: String, user_id: String, scope: String,
                             expires_in: Int, email: String, verified_email: Boolean, access_type: String)
