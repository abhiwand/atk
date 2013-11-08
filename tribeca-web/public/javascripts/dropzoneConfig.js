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
            success:function(file){
                file.previewElement.remove();
                $("#uploadedFiles tbody").append("<tr><td>" + file.name + "</td><td>" + Dropzone.formatSize(file.size) + "</td></tr>")
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

    $(".file-remove").click(function(event){
        $.ajax
        ({
            type: "POST",
            url: "files/delete",
            dataType: 'json',
            contentType: "application/json",
            data: JSON.stringify({"name": event.currentTarget.parentNode.parentNode.children[0].innerText}),
            success: function (data) {
                console.log(data)
            },
            error: function(){
                console.log("error")
            }
        })
    })
})

var deleteFile = function(fileName){


}