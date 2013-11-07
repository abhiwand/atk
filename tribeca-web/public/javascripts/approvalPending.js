$(window).load(function(){
    if($.cookie("approvalPending") != undefined && $.cookie("approvalPending") != ""){
        $("#approvalPending").removeClass("hidden")
        $.removeCookie("approvalPending")
    }
    if($.cookie("authenticationFailed") != undefined && $.cookie("authenticationFailed") != ""){
        $("#authenticationFailed").removeClass("hidden")
        $.removeCookie("authenticationFailed")
    }
})