var myDropZone = "";
$(window).ready(function(){
    Dropzone.autoDiscover = false;
    Dropzone.formatSize = function(bytes) {
        var sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
        if (bytes == 0) return 'n/a';
        var sizeIndex = parseInt(Math.floor(Math.log(bytes) / Math.log(1024)));
        return Math.round(bytes / Math.pow(1024, sizeIndex), 2) + ' ' + sizes[sizeIndex];
    }

    if($("#uploadForm").length > 0){
        myDropZone = new Dropzone("#uploadForm", {
            maxFilesize: 5120,
            clickable: true,
            init:function(){
                $("#uploadForm").addClass("dropzone")
            },
            addRemoveLinks:true,
            createImageThumbnails:false,
            acceptedFiles:".csv",
            success:function(file){
                $.ajax({
                    type: "POST",
                    url: "files/create",
                    dataType: 'json',
                    contentType: "application/json",
                    data: JSON.stringify({"name": file.name, "size": file.size}),
                    success: function (data) {
                        console.log(data)
                    }
                })

                var interval = setInterval(function(){
                    $.ajax({
                        type: "POST",
                        url: "files/progress",
                        dataType: "json",
                        contentType: "application/json",
                        data: JSON.stringify({"name": file.name, "size": file.size}),
                        success: function(data){
                            if(data && data != undefined && data.progress != undefined){
                                var progress = data.progress/2;
                                if(data.progress >= 100){
                                    file.previewElement.querySelector("[data-dz-uploadprogress]").style.width = "100%";
                                    myDropZone.removeFile(file);
                                    $(".file-remove").unbind()
                                    $("#uploadedFiles tbody").append("<tr><td>" + file.name + "</td><td>" + Dropzone.formatSize(file.size) + "</td></tr>")
                                    bindDelete()
                                    clearInterval(interval)
                                }else{
                                    file.previewElement.querySelector("[data-dz-uploadprogress]").style.width = "" + (progress +50) + "%";
                                    console.log(progress)
                                }

                            }
                        }, timeout: 1400 });
                }, 1500);

                /*(function poll(){
                    $.ajax({
                        type: "POST",
                        url: "files/progress",
                        dataType: "json",
                        contentType: "application/json",
                        data: JSON.stringify({"name": file.name, "size": file.size}),
                        success: function(data){
                            //Update your dashboard gauge
                            console.log(data)

                        }, complete: poll, timeout: 30000 });
                })();*/

                //file.previewElement.remove();
                //myDropZone.removeFile(file);
                //$(".file-remove").unbind()
                /*$("#uploadedFiles tbody").append("<tr><td>" + file.name + "</td><td>" + Dropzone.formatSize(file.size) + "</td></tr>")
                bindDelete()
                */

            }
        });
    }

    bindDelete();
})

var bindDelete = function(){
    $(".file-remove").click(function(event){
        deleteFile(event)
    })
}
var deleteFile = function(event){
    $.ajax
    ({
        type: "POST",
        url: "files/delete",
        dataType: 'json',
        contentType: "application/json",
        data: JSON.stringify({"name": event.currentTarget.parentNode.parentNode.children[0].innerText}),
        success: function (data) {
            if(data.file != undefined && data.file != null){
            $("#uploadedFiles tbody tr").each(function(index, dom){
                if( $(dom).children()[0].innerText.trim().toLocaleLowerCase() === data.file.trim().toLocaleLowerCase()){
                    $(dom).remove();
                }
            });
            }
        },
        error: function(){
            console.log("error")
        }
    })

}