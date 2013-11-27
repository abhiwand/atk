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
                //file.previewElement.remove();
                myDropZone.removeFile(file);
                $(".file-remove").unbind()
                $("#uploadedFiles tbody").append("<tr><td>" + file.name + "</td><td>" + Dropzone.formatSize(file.size) + "</td></tr>")
                bindDelete()
                $.ajax({
                        type: "POST",
                        url: "files/create",
                        dataType: 'json',
                        contentType: "application/json",
                        data: JSON.stringify({"name": file.name, "size": file.size}),
                        success: function (data) {
                            console.log(data)
                        },
                        error: function(){
                            console.log("error")
                        }
                    })

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