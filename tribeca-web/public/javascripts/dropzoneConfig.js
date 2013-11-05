var myDropZone = "";
$(window).ready(function(){
    myDropZone = new Dropzone("#uploadForm", {
        maxFilesize: 5120,
        clickable: true,
        init:function(){
            $("#uploadForm").addClass("dropzone")
        }
    });
    /*$("div#upload-form").dropzone(
        {
            maxFilesize: 5120,
            clickable: true
        });*/
/*    Dropzone.options.uploadForm = {
        paramName: "file", // The name that will be used to transfer the file
        maxFilesize: 5120, // MB
        accept: function(file, done) {
            if (file.name == "justinbieber.jpg") {
                done("Naha, you don't.");
            }
            else { done(); }
        }
    };*/
})