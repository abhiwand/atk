$(window).load(function(){
    if($.cookie("approvalPending") != undefined && $.cookie("approvalPending") != ""){
        $("#approvalPending").removeClass("hidden")
        $.removeCookie("approvalPending")
    }
})