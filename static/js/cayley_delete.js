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

  $("#delete_object").click(function() {
    var subject = $("#object").val()
    var gremlin_predicate_in = "g.V('" + subject + "').In(g.V(), 'pred').All()"
    var gremlin_predicate_out = "g.V('" + subject + "').Out(g.V(), 'pred').All()"

    $.when(
      $.post("/api/v1/query/gremlin", gremlin_predicate_out),
      $.post("/api/v1/query/gremlin", gremlin_predicate_in)
    ).then(function(pred_out_data, pred_in_data) {

        var remove_list = _getParsedResult(pred_out_data[0], subject, false).concat(
          _getParsedResult(pred_in_data[0], false, subject))

        $.post("/api/v1/delete", JSON.stringify(remove_list)).done(function() {
          $("#status").prepend('[' + subject + '] removed ' + remove_list.length + ' items <hr>')
        })

    }, function(pred_out_data, pred_in_data) {
        $("#status").prepend('[' + subject + '] failed <hr>')
      });
  });

  _getParsedResult = function(data, subject, object) {
    var _list = []

    output_json = JSON.parse(data)
    item_list = output_json['result']

    if(item_list) {
      item_list.forEach(function(n) {
        _list.push({
          "subject": subject ? subject : n.id,
          "predicate": n.pred,
          "object": object ? object : n.id
        })
      })
    }

    return _list;
  }

});
