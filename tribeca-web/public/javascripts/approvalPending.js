$(window).load(function(){
    if($.cookie("approvalPending") != undefined && $.cookie("approvalPending") != ""){
        //trigger message
        alert("approal pending");
    }
})