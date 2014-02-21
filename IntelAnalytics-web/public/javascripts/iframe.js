function setIframeHeight(){
    if($('#ipython').length > 0){
        $("#ipython").css("height", ($(window).height()-50) + "px");
        $("#main").css("overflow", "hidden");
        $("body").css("overflow", "hidden");
    }
}

$(document).ready(function(){
    console.log("ready " + $(window).height())
    setIframeHeight();
});

$(window).resize(function(){
    console.log("resize " + $(window).height())
    setIframeHeight();
})
