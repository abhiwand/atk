(function() {
    var po = document.createElement('script');
    po.type = 'text/javascript'; po.async = true;
    po.src = 'https://apis.google.com/js/client:plusone.js';
    var s = document.getElementsByTagName('script')[0];
    s.parentNode.insertBefore(po, s);
    })();

$(window).load(function(){
    /*$("#googleRegisterButton").click(function(){
        googleAuth.registerRender()
    })*/
})
var googleAuth = {
    clientId:{
    "http:":"141308260505-d7utft9orcofca75fkspuit96ordo8dm.apps.googleusercontent.com",
    "https:":"141308260505-jf332k2mi49jggi2cugf08vk17u9s9rk.apps.googleusercontent.com"
    },
    loginRender: function(){
        gapi.signin.render('googleLoginButton', {
            'callback': 'loginCallback',
            'clientid': this.clientId[window.location.protocol],
            'cookiepolicy': 'single_host_origin',
            'requestvisibleactions': 'http://schemas.google.com/AddActivity',
            'scope': 'https://www.googleapis.com/auth/userinfo.profile https://www.googleapis.com/auth/userinfo.email'
        });
    },
    registerRender: function(form){
    gapi.signin.render('googleRegisterButton', {
        'callback': 'registerCallback',
        'clientid': this.clientId[window.location.protocol],
        'cookiepolicy': 'single_host_origin',
        'requestvisibleactions': 'http://schemas.google.com/AddActivity',
        'scope': 'https://www.googleapis.com/auth/userinfo.profile https://www.googleapis.com/auth/userinfo.email'
    });
    }
}

var registerCallback = function(authResult) {
    authAjax(authResult,"registration-form","ipython", "register")
}

var loginCallback = function(authResult){
    authAjax(authResult,null, "login")
}

var authAjax =  function(authResult, formId,redirectUrl, url){
    console.log('signinCallback');
    console.log(authResult);
    var obj = {};
    obj.auth = authResult;
    obj.form = {};
    if(formId != undefined){
        obj.form = $("#"+formId).serializeArray()
    }

    if (authResult['access_token']) {
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
            data: JSON.stringify(obj),
            //data: authResult,
            success: function () {
                if(redirectUrl != undefined){
                    window.location.replace( window.location.protocol + "//" + window.location.host+ "/" + redirectUrl)
                }
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