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

  $("#delete_node").click(function() {
    var vertex = $("#node").val()
    var gremlin_predicate_in = "g.V('" + vertex + "').In(g.V(), 'pred').All()"
    var gremlin_predicate_out = "g.V('" + vertex + "').Out(g.V(), 'pred').All()"

    $.when(
      $.post("/api/v1/query/gremlin", gremlin_predicate_out),
      $.post("/api/v1/query/gremlin", gremlin_predicate_in)
    ).then(function(predOutData, predInData) {

        var tripleList = _getParsedResult(predOutData[0], vertex, false).concat(
          _getParsedResult(predInData[0], false, vertex))

        $.post("/api/v1/delete", JSON.stringify(tripleList))
          .done(function() {
            $("#status").prepend('[' + vertex + '] removed ' + tripleList.length + ' items <hr>')
          })
          .fail(function(jqxhr, textStatus, errorThrown) {
            $("#status").prepend('[' + vertex + '] ' + jqxhr.responseText + ' <hr>')
          })

    }, function(predOutData, predInData) {
        $("#status").prepend('[' + vertex + '] failed during query for data<hr>')
      });
  });

  _getParsedResult = function(data, subject, object) {
    var _tripleList = []

    _dataJson = JSON.parse(data)
    if(itemList = _dataJson['result']) {
      itemList.forEach(function(item) {
        _tripleList.push({
          "subject": subject ? subject : item.id,
          "predicate": item.pred,
          "object": object ? object : item.id
        })
      })
    }

    return _tripleList;
  }

});
