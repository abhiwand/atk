$(window).load(function(){
    if($.cookie("approvalPending") != undefined && $.cookie("approvalPending") != ""){
        $("#approvalPending").removeClass("hidden")
        $.removeCookie("approvalPending")
    }
    if($.cookie("authenticationFailed") != undefined && $.cookie("authenticationFailed") != ""){
        $("#authenticationFailed").removeClass("hidden")
        $.removeCookie("authenticationFailed")
    }
    if($.cookie("registered") != undefined && $.cookie("registered") != ""){
        $("#requestInvite").remove();
        $("#request-an-invite-modal-slider1").remove();
        $("#request-an-invite-modal-slider2").remove();
        $("#request-an-invite-modal-slider3").remove();
    }
    messages.getMessagesHtml();
})

var messages = {
    approvalPending:"",
    registrationRequired:"",
    getMessagesHtml: function(){
        messages.approvalPending = $("#approvalPendingParent").html();
        messages.registrationRequired = $("#registrationRequiredParent").html();
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
    }

}