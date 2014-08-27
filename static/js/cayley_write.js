// Copyright 2014 The Cayley Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

$(function() {


  var alertFail = function(text) {
    $("#alertBox").text(text)
    $("#alertBox").addClass("alert-danger").fadeIn(300).delay(2000).fadeOut(300).queue(function(){ $(this).removeClass("alert-danger")});
  }

  var alertSucceed = function(text) {
    $("#alertBox").text(text)
    $("#alertBox").addClass("alert-success").fadeIn(300).delay(2000).fadeOut(300).queue(function(){ $(this).removeClass("alert-success")});
  }

  var checkQuad = function(t) {
    if (t.subject == "") {
      alertFail("Need a subject")
      return false
    }
    if (t.predicate == "") {
      alertFail("Need a predicate")
      return false
    }
    if (t.object == "") {
      alertFail("Need an object")
      return false
    }
    return true
  }

  $("#sbWrite").addClass("active");

  $("#add_quad").click(function() {
    var quad = {
      subject: $("#subject").val(),
      predicate: $("#predicate").val(),
      object: $("#object").val(),
      label: $("#label").val()
    }
    if (!checkQuad(quad)) {
      return
    }
    $.post("/api/v1/write", JSON.stringify([quad]))
      .done(function(return_data){
        alertSucceed("Wrote a quad!")
      })
      .fail(function(jqxhr) {
        var data = $.parseJSON(jqxhr.responseText)
        alertFail(data.error)
      })
  })

  $("#delete_quad").click(function() {
    var quad = {
      subject: $("#rsubject").val(),
      predicate: $("#rpredicate").val(),
      object: $("#robject").val(),
      label: $("#rlabel").val()
    }
    if (!checkQuad(quad)) {
      return
    }
    $.post("/api/v1/delete", JSON.stringify([quad]))
      .done(function(return_data){
        alertSucceed("Deleted a quad!")
      })
      .fail(function(jqxhr) {
        var data = $.parseJSON(jqxhr.responseText)
        alertFail(data.error)
      })
  })

  var upload = null

  var uploadProgress = function(event) {
    if (event.lengthComputable) {
      var percentComplete = Math.round(event.loaded * 100 / event.total);
      $("#alertBox").text("Uploaded  " + percentComplete.toString() + "%")
    } else {
      $("#alertBox").text("Uploading...")
    }
  }

  var uploadComplete = function(event) {
    stopAndReset();
    var data = $.parseJSON(upload.responseText)
    $("#alertBox").text("Upload complete! " + data.result);
    $("#alertBox").removeClass("alert-info").addClass("alert-success")
    .delay(2000).fadeOut(300)
    .queue(function () {
      $("#write_file").fadeIn(30);
      $("#alertBox").removeClass("alert-success");
    })
    upload = null
  }

  var uploadCanceled = function(event) {
    upload = null
    stopAndReset();
    $("#alertBox").text("Upload canceled!")
    $("#alertBox").removeClass("alert-info").addClass("alert-danger")
    .delay(2000).fadeOut(300)
    .queue(function () {
      $("#write_file").fadeIn(30);
      $("#alertBox").removeClass("alert-danger");
    })
  }

  var uploadFailed = function(event) {
    upload = null
    stopAndReset();
    $("#alertBox").text("Upload failed!")
    $("#alertBox").removeClass("alert-info").addClass("alert-danger")
    .delay(2000).fadeOut(300)
    .queue(function () {
      $("#write_file").fadeIn(30);
      $("#alertBox").removeClass("alert-danger");
    })
  }

  $("#write_file").click(function() {
    try {
      animate();
      var fd = new FormData()
      fd.append("NQuadFile", document.getElementById("nquad_file").files[0])
      var xhr = new XMLHttpRequest()
      upload = xhr
      $("#write_file").fadeOut(30);
      $("#alertBox").addClass("alert-info").fadeIn(300);
      xhr.upload.addEventListener("progress", uploadProgress, false);
      xhr.addEventListener("load", uploadComplete, false);
      xhr.addEventListener("error", uploadFailed, false);
      xhr.addEventListener("abort", uploadCanceled, false);
      xhr.open("POST", "/api/v1/write/file/nquad");
      xhr.send(fd);

    } catch(err) {
      $("#alertBox").removeClass("alert-info");
      stopAndReset();
      alertFail(err)
      $("#write_file").fadeIn(30);
    }
  })

});
