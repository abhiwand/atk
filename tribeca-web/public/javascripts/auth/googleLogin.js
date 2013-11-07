(function() {
    var po = document.createElement('script');
    po.type = 'text/javascript'; po.async = true;
    po.src = 'https://apis.google.com/js/client.js';
    var s = document.getElementsByTagName('script')[0];
    s.parentNode.insertBefore(po, s);
    })();

var googleAuth;
$(window).load(function(){
    googleAuth = {
        apiKey:GOOGLEOAUTH.apiKey,
        scope: GOOGLEOAUTH.scope,
        clientId: GOOGLEOAUTH.clientId,
        loginButtonId:"googleLoginButton",
        handleClientLoad: function(){
            gapi.client.setApiKey(googleAuth.apiKey)
            googleAuth.checkAuth(googleAuth.handleAuthResultLogin)
        },
        checkAuth: function(callback) {
            gapi.auth.authorize({client_id: googleAuth.clientId, scope: googleAuth.scope, immediate: true}, callback);
        },
        handleAuthResultLogin: function(authResult) {
            var authorizeButton = document.getElementById(googleAuth.loginButtonId);
            if( authorizeButton != null && authorizeButton != undefined){
                if (authResult && !authResult.error) {
                    authorizeButton.onclick = googleAuth.makeApiCallLogin;//googleAuth.makeOauthApiCall(googleAuth.loginCallback);//this.makeApiCallLogin;
                } else {
                    authorizeButton.onclick = googleAuth.handleAuthClickLogin;//(googleAuth.makeApiCallLogin);//this.handleAuthClickLogin;
                }
            }
        },
        handleAuthResultRegister: function(authResult) {
            if (authResult && !authResult.error) {
                googleAuth.makeApiCallRegister();
            } else {
                googleAuth.handleAuthClickRegister();
            }
        },
        makeOauthApiCall: function(callback){
            gapi.client.load('oauth2', 'v2', function() {
                var request = gapi.client.oauth2.userinfo.get();
                request.execute(function(resp) {
                    callback(resp);
                });
            });
        },
        makeApiCallLogin: function() {
            googleAuth.makeOauthApiCall(googleAuth.loginCallback);
        },
        loginCallback: function(authResult){
            if(isValidApiResponse(authResult)){
                googleAuth.authAjax(authResult,null, "login")
            }else{
                showSignInError()
            }
        },
        makeApiCallRegister: function() {
            googleAuth.makeOauthApiCall(googleAuth.registerCallback)
        },
        registerCallback: function(authResult) {
            if(isValidApiResponse(authResult)){
                $("#registerAuthResult").attr("value", JSON.stringify(authResult))
                googleAuth.registerSubmit()
            }else{
                showSignInError()
            }
        },
        handleAuthClick: function(callback){
            gapi.auth.authorize({client_id: this.clientId, scope: this.scope, immediate: false}, callback);
            return false;
        },
        handleAuthClickLogin: function() {
            this.handleAuthClick(this.makeApiCallLogin);
            return false;
        },
        handleAuthClickRegister: function() {
            this.handleAuthClick(this.makeApiCallRegister)
            return false;
        },
/*        loginRender: function(){
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
        },*/
        registerSubmit: function(){
            $("#registration-form").submit()
        },
        authRedirect: function(data){
            if(data != undefined && data.code != undefined && data.url != undefined){
                window.location.replace( window.location.protocol + "//" + window.location.host+ "/" + data.url)
            }
            return false;
        },
        authAjax: function(authResult, redirectUrl, url){
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
                        googleAuth.authRedirect(data)
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

            }
        }
    }

    googleAuth.handleClientLoad();
    $("#googleLoginButton").click(function(){
        googleAuth.checkAuth(googleAuth.handleAuthResultLogin)
    })


})
/*
function handleClientLoad() {
    gapi.client.setApiKey(GOOGLEOAUTH.apiKey)
    checkAuth(handleAuthResultLogin)
    //checkAuth(handleAuthResultRegister)
}*/

/*function checkAuth(callback) {
    gapi.auth.authorize({client_id: googleAuth.clientId[window.location.protocol][window.location.port],
        scope: googleAuth.scope,
        immediate: true}, callback);
}*/

/*function handleAuthClickLogin() {
    gapi.auth.authorize({client_id: googleAuth.clientId[window.location.protocol][window.location.port], scope: googleAuth.scope, immediate: false},
        makeApiCallLogin);
    return false;
}*/
/*function handleAuthClickRegister() {
    gapi.auth.authorize({client_id: googleAuth.clientId[window.location.protocol][window.location.port], scope: googleAuth.scope, immediate: false},
        makeApiCallRegister);
    return false;
}*/

/*var makeApiCallRegister = function() {
    gapi.client.load('oauth2', 'v2', function() {
        var request = gapi.client.oauth2.userinfo.get();
        request.execute(function(resp) {
            registerCallback(resp);
        });
    });
}*/
/*
var makeApiCallLogin = function() {
    gapi.client.load('oauth2', 'v2', function() {
        var request = gapi.client.oauth2.userinfo.get();
        request.execute(function(resp) {
            loginCallback(resp);
        });
    });
}*/
/*
var authRedirect = function(data){
    if(data != undefined && data.code != undefined && data.url != undefined){
        window.location.replace( window.location.protocol + "//" + window.location.host+ "/" + data.url)
    }
    return false;
}*/

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

/*
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
                googleAuth.authRedirect(data)
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

    }

}*/
