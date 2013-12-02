$(window).load(function(){
    messages.getMessagesHtml();

    if($.cookie("approvalPending") != undefined && $.cookie("approvalPending") != ""){
        $("#confirmationModal").modal('show')
        $.removeCookie("approvalPending")
    }
    if($.cookie("authenticationFailed") != undefined && $.cookie("authenticationFailed") != ""){
        messages.showAuthFailed()
        $.removeCookie("authenticationFailed")
    }
    if($.cookie("registered") != undefined && $.cookie("registered") != ""){
        /*$("#requestInvite").remove();
        $("#request-an-invite-modal-slider1").remove();
        $("#request-an-invite-modal-slider2").remove();
        $("#request-an-invite-modal-slider3").remove();*/
    }

    menuMarker();
})

function menuMarker(){
    var path = window.location.pathname.split("/");
    var pathName = path[path.length-1];

    switch (pathName.toLowerCase()){
        case "ipython":
            $("#ipython-page").addClass("active");
        case "documentation":
            $("#documentation-page").addClass("active");


    }
}
var messages = {
    approvalPending:"",
    registrationRequired:"",
    authFailed:"",
    getMessagesHtml: function(){
        messages.approvalPending = $("#approvalPendingParent").html();
        messages.registrationRequired = $("#registrationRequiredParent").html();
        messages.authFailed = $("#authenticationFailedParent").html();
    },
    showApprovalPending: function(){
        $("#approvalPending").remove();
        $("body").append(messages.approvalPending)
        $("#approvalPending").removeClass("hidden")
    },
    showRegistrationRequired: function(){
        $("#registrationRequired").remove();
        $("body").append(messages.registrationRequired)
        $("#registrationRequired").removeClass("hidden")
    },
    showAuthFailed: function(){
        $("#authenticationFailed").remove();
        $("body").append(messages.authFailed)
        $("#authenticationFailed").removeClass("hidden")
    }


}