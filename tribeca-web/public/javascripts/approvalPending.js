$(window).load(function(){
    if($.cookie("approvalPending") != undefined && $.cookie("approvalPending") != ""){
        $("#approvalPending").removeClass("hidden")
        $.removeCookie("approvalPending")
    }
    if($.cookie("authenticationFailed") != undefined && $.cookie("authenticationFailed") != ""){
        $("#authenticationFailed").removeClass("hidden")
        $.removeCookie("authenticationFailed")
    }
    messages.getMessageHtml();
})

var messages = {
    approvalPending:"",
    getMessageHtml: function(){
        messages.approvalPending = $("#approvalPendingParent").html();
    },
    showApprovalPending: function(){
        $("#approvalPending").remove();
        $("body").append(messages.approvalPending)
        $("#approvalPending").removeClass("hidden")
    }

}