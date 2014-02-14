var myDropZone = "";
$(window).ready(function(){
    Dropzone.autoDiscover = false;
    Dropzone.formatSize = function(bytes) {
        var sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
        if (bytes == 0) return 'n/a';
        var sizeIndex = parseInt(Math.floor(Math.log(bytes) / Math.log(1024)));
        return Math.round(bytes / Math.pow(1024, sizeIndex), 2) + ' ' + sizes[sizeIndex];
    }
    Dropzone.bindDelete = function(){
        $(".file-remove").click(function(event){
            deleteFile(event)
        })
    }
    Dropzone.deleteFile = function(event){
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
    Dropzone.poll = function(file){
        if(Dropzone.file.valid(file)){
            Dropzone.polls[file.name] = setInterval(function(){
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
                                Dropzone.poll.clear(file)
                                file.previewElement.classList.add("dz-success")
                                file.previewElement.querySelector("[data-dz-uploadprogress]").style.width = "100%";
                                var timeout = window.setTimeout(function(){
                                    //myDropZone.removeFile(file);
                                    file.previewElement.remove()
                                    $(".file-remove").unbind()
                                    Dropzone.file.add(file)
                                    Dropzone.bindDelete()

                                }, 3000 )

                            }else{
                                file.previewElement.querySelector("[data-dz-uploadprogress]").style.width = "" + (progress +50) + "%";
                                console.log(progress)
                            }

                        }
                    }, timeout: 1400 });
            }, 1500)
        }

    }
    if(Dropzone.file == undefined){
        Dropzone.file = {}
    }
    Dropzone.file.valid = function(file){
        if(file && file != undefined && (file.name != undefined && file.name != "") && (file.size != undefined && file.size != 0)){
            return true;
        }else{
            return false;
        }
    }
    Dropzone.file.add = function(file){
        if(Dropzone.file.valid(file) && !Dropzone.file.find(file)){
            $("#uploadedFiles tbody").append("<tr><td>" + file.name + "</td><td>" + Dropzone.formatSize(file.size) + "</td></tr>")
        }
    }
    Dropzone.file.find = function(file){
        if(Dropzone.file.valid(file)){
           var list =  $("#uploadedFiles tbody tr");
            for(var i in list){
                if(!isNaN(i-0)){
                    var fileName = $(list[i]).find("td").eq(0).text()
                    if( file.name.trim() ===  fileName ){
                        return true;
                    }
                }
            }
        }
        return false;
    }

    Dropzone.polls = {}
    Dropzone.poll.clear = function(file){
        if(Dropzone.file.valid(file)){
            clearInterval(Dropzone.polls[file.name])
        }
    }

    if($("#uploadForm").length > 0){
        myDropZone = new Dropzone("#uploadForm", {
            maxFilesize: 5120,
            clickable: true,
            init:function(){
                $("#uploadForm").addClass("dropzone")
            },
            addRemoveLinks:false,
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

                Dropzone.poll(file)
            }
        });
    }

    //Dropzone.bindDelete();
})

