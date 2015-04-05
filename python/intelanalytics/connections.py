# configuration to reach various servers


# (define oauth servers first, as they are dependencies of ia_server)

# Oregon UAA
class oauth_or:  # (see server_defaults to disable entirely)
    scheme = "https"
    host = "login.gotapaas.com"
    port = None
    headers = {"Accept": "application/json"}
    # user_name and user_password come from ia_server
    # Must specify client name and password as well:
    client_name = "atk-client"
    client_password = "c1oudc0w"


# Ireland UAA
class oauth_eu:  # (see server_defaults to disable entirely)
    scheme = "https"
    host = "uaa.run.gotapaas.eu"
    port = None
    headers = {"Accept": "application/json"}
    # user_name and user_password come from ia_server login
    # Must specify client name and password as well:
    client_name = "atk-client"
    client_password = "c1oudc0w"



class ia_server_local:
    """Standard, local server"""
    scheme = "http"
    host = "localhost"
    port = 9099
    user_name = "test_api_key_1"
    version = "v1"
    oauth_server = None  #oauth_eu


class ia_server_eu:
    """Ireland ATK server"""
    scheme = "http"
    host = "atk-bryn2.apps.gotapaas.eu"
    port = None
    user_name = "admin"
    user_password="c1oudc0w"
    version = "v1"
    oauth_server = oauth_eu
    #user_name = "admin"   # oregon
    #user_password = "WQXng43TEfj"
    #oauth_server=oauth_or


ia_server = ia_server_local
#ia_server = ia_server_eu



