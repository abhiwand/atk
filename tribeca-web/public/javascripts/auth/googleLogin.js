(function() {
    var po = document.createElement('script');
    po.type = 'text/javascript'; po.async = true;
    po.src = 'https://apis.google.com/js/client.js';
    var s = document.getElementsByTagName('script')[0];
    s.parentNode.insertBefore(po, s);
    })();

$(window).load(function(){
    handleClientLoad();
    $("#googleLoginButton").click(function(){
        checkAuth(handleAuthResultLogin)
    })
})

function handleClientLoad() {
    gapi.client.setApiKey(googleAuth.apiKey)
    checkAuth(handleAuthResultLogin)
    //checkAuth(handleAuthResultRegister)
}

function checkAuth(callback) {
    gapi.auth.authorize({client_id: googleAuth.clientId[window.location.protocol][window.location.port],
        scope: googleAuth.scope,
        immediate: true}, callback);
}

var handleAuthResultRegister = function(authResult) {
    if (authResult && !authResult.error) {
        makeApiCallRegister();
    } else {
        handleAuthClickRegister();
    }
}

var handleAuthResultLogin = function(authResult) {
    var authorizeButton = document.getElementById('googleLoginButton');
    if( authorizeButton != null && authorizeButton != undefined){
        if (authResult && !authResult.error) {
            authorizeButton.onclick = makeApiCallLogin;
        } else {
            authorizeButton.onclick = handleAuthClickLogin;
        }
    }
}

function handleAuthClickLogin() {
    gapi.auth.authorize({client_id: googleAuth.clientId[window.location.protocol][window.location.port], scope: googleAuth.scope, immediate: false},
        makeApiCallLogin);
    return false;
}
function handleAuthClickRegister() {
    gapi.auth.authorize({client_id: googleAuth.clientId[window.location.protocol][window.location.port], scope: googleAuth.scope, immediate: false},
        makeApiCallRegister);
    return false;
}

var makeApiCallRegister = function() {
    gapi.client.load('oauth2', 'v2', function() {
        var request = gapi.client.oauth2.userinfo.get();
        request.execute(function(resp) {
            registerCallback(resp);
        });
    });
}

var makeApiCallLogin = function() {
    gapi.client.load('oauth2', 'v2', function() {
        var request = gapi.client.oauth2.userinfo.get();
        request.execute(function(resp) {
            loginCallback(resp);
        });
    });
}

var googleAuth = {
    apiKey:"AIzaSyCRdEDqRgF3X_6ZeL2RraPOx4SPAiQ5Of8",
    scope: "https://www.googleapis.com/auth/plus.me https://www.googleapis.com/auth/userinfo.profile https://www.googleapis.com/auth/userinfo.email",
    clientId:{ "http:" : {"9000":"141308260505-d7utft9orcofca75fkspuit96ordo8dm.apps.googleusercontent.com"},
    "https:" : { "9443":"141308260505-jf332k2mi49jggi2cugf08vk17u9s9rk.apps.googleusercontent.com",
                "": "141308260505-3qf2ofckirolrkajt3ansibkuk5qug5t.apps.googleusercontent.com"}
    },
    loginRender: function(){
        gapi.signin.render('googleLoginButton', {
            'callback': 'loginCallback',
            'clientid': this.clientId[window.location.protocol][window.location.port],
            'cookiepolicy': 'single_host_origin',
            'immediate': true,
            'requestvisibleactions': 'http://schemas.google.com/AddActivity',
            'scope': 'https://www.googleapis.com/auth/userinfo.profile https://www.googleapis.com/auth/userinfo.email'
        });
    },
    registerRender: function(){
    gapi.signin.render('googleRegisterButton', {
        'callback': 'registerCallback',
        'clientid': this.clientId[window.location.protocol][window.location.port],
        'cookiepolicy': 'single_host_origin',

        'requestvisibleactions': 'http://schemas.google.com/AddActivity',
        'scope': 'https://www.googleapis.com/auth/userinfo.profile https://www.googleapis.com/auth/userinfo.email'
    });
    },
    registerSubmit: function(){
        $("#registration-form").submit()
    }
}

var authRedirect = function(data){
    if(data != undefined && data.code != undefined && data.url != undefined){
        window.location.replace( window.location.protocol + "//" + window.location.host+ "/" + data.url)
    }
    return false;
}

var registerCallback = function(authResult) {
    if(isValidApiResponse(authResult)){
        $("#registerAuthResult").attr("value", JSON.stringify(authResult))
        googleAuth.registerSubmit()
    }else{
        showSignInError()
    }
}

var showSignInError = function(){
    $("#loginError").removeClass("hidden");
}

var isValidApiResponse = function(resp){
    if(resp == undefined || resp == null){
        return false;
    }else{
        return true;
    }
}
var isValidAuthResult = function(authResult){
    if(authResult == undefined || authResult.access_token == undefined || authResult.access_token === ""){
        return false;
    }else{
        return true;
    }
}
var loginCallback = function(authResult){
    if(isValidApiResponse(authResult)){
        authAjax(authResult,null, "login")
    }else{
        showSignInError()
    }
}

var authAjax =  function(authResult, redirectUrl, url){
    console.log(authResult);
    var obj = {};
    //obj.auth = authResult;
    if (authResult['email']) {
        // Successfully authorized
        // Hide the sign-in button now that the user is authorized, for example:
        $.ajax
        ({
            type: "POST",
            //the url where you want to sent the userName and password to
            url: url,
            dataType: 'json',
            contentType: "application/json",
            async: false,
            //json object to sent to the authentication url
            data: JSON.stringify(authResult),
            //data: authResult,
            success: function (data) {
                console.log(data)
                authRedirect(data)
                if(redirectUrl != undefined){
                    console.log("forward: " + redirectUrl)
                    //window.location.replace( window.location.protocol + "//" + window.location.host+ "/" + redirectUrl)
                }
            },
            error: function(one, two){
                console.log("error")
                console.log(one)
                console.log(two)
            }
        })
    } else if (authResult['error']) {
        // There was an error.
        // Possible error codes:
        //   "access_denied" - User denied access to your app
        //   "immediate_failed" - Could not automatically log in the user
        // console.log('There was an error: ' + authResult['error']);
    }

}