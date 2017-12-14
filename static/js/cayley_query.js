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

  $("#sbQuery").addClass("active");

  output_editor = CodeMirror.fromTextArea(document.getElementById("output"), {
    lineNumbers: true,
    matchBrackets: true,
  });

  $("#run_button").click(function() {
    var data = editor.getValue()
    $("#output").text(editor.getValue())
    animate();
    $.post("/api/v1/query/" + selectedQueryLanguage, data)
      .done(function(return_data) {
        if (typeof(Storage) !== "undefined") {
          localStorage.setItem("cayleySavedQueries" + selectedQueryLanguage, data)
        }
        output_editor.setValue(JSON.stringify(JSON.parse(return_data), null, '\t'))
        stopAndReset();
      })
      .fail(function(jqxhr, textStatus, errorThrown){
        output_editor.setValue(jqxhr.responseText)
        stopAndReset();
      })
  })
});
