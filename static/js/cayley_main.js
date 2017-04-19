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
  s = null;
  group = null;
  Snap.load("/static/cayley.svg", function(d, err) {
    //Snap(105,65).append(d);
    s = Snap("#logo").append(d);
    svg = Snap("svg");
    var c = svg.selectAll("*")
    group = svg.group()
    group.add(c)
    scale = new Snap.Matrix();
    scale.scale(0.5);
    group.transform(scale)
  })

	neutralColor = "#999999"
	green = "#0F9D58"

	stopAndReset = function () {
		for (var i = 0; i < 19; i++) {
			group[i].stop()
		}
		for (var i = 0; i < currentTimeouts.length; i++) {
			clearTimeout(currentTimeouts[i])
		}
		currentTimeouts = []
		group[17].attr({fill: neutralColor})
		group[18].attr({fill: neutralColor})
		for (var i = 1; i < 11; i++) {
			group[i].attr({stroke: neutralColor})
		}

	}

	currentTimeouts = []

	flash = function(elem, start) {
		currentTimeouts.push(
			setTimeout(function() { elem.animate({stroke: green}, 1000)}, start * 1000))
		currentTimeouts.push(
			setTimeout(function() { elem.animate({stroke: neutralColor}, 1200)}, (start + 1) * 1000))
	}

	animate = function() {
		group[17].animate({fill: green}, 1000)
		currentTimeouts.push(
			setTimeout(function() {group[17].animate({fill: neutralColor}, 1200)}, 1000))
		flash(group[1], 0.5)
		flash(group[8], 1.0)
		flash(group[4], 1.5)
		flash(group[10], 1.0)
		flash(group[6], 1.5)
		flash(group[2], 2.0)
		currentTimeouts.push(
			setTimeout(function() {group[18].animate({fill: green}, 1000)}, 2500))
		currentTimeouts.push(
			setTimeout(function() {group[18].animate({fill: neutralColor}, 1200)}, 3500))
		currentTimeouts.push(
			setTimeout(function() {group[17].animate({fill: green}, 1000)}, 3500))
		currentTimeouts.push(
			setTimeout(function() {group[17].animate({fill: neutralColor}, 1200)}, 4500))
		flash(group[1], 4.0)
		flash(group[7], 4.5)
		flash(group[5], 5.0)
		flash(group[9], 4.5)
		flash(group[3], 5.0)
		flash(group[2], 5.5)
		currentTimeouts.push(
			setTimeout(function() {group[18].animate({fill: green}, 1000)}, 6000))
		currentTimeouts.push(
			setTimeout(function() {group[18].animate({fill: neutralColor}, 1200, function() {
				stopAndReset();
				animate();
		})}, 7000))
	}

  if ($("#code").length != 0) {
    editor = CodeMirror.fromTextArea(document.getElementById("code"), {
      lineNumbers: true,
      matchBrackets: true,
      continueComments: "Enter",
      //        extraKeys: {"Ctrl-Q": "toggleComment"}
    });
  } else{
    editor = null;
  }

  var defaultQueryStrings = {
    "gizmo": "g.Emit('Hello World')",
    "mql": "[{\n  \"id\": \"Hello World\"\n}]",
    "graphql": "{\n  nodes(first: 10){\n    id\n  }\n}"
  }

  var getLastQueryStringFor = function(type) {
    if (typeof(Storage) !== "undefined") {
      return localStorage.getItem("cayleySavedQueries" + type)
    } else {
      return defaultQueryStrings[type]
    }
  }

  var switchTo = function(type) {
    if (type === "gizmo") { switchToGizmo()}
    if (type === "mql") { switchToMQL()}
    if (type === "graphql") { switchToGraphQL()}
    if (typeof(Storage) !== "undefined") {
      localStorage.setItem("cayleyQueryLang", type);
    }
    if (editor) {
      editor.setValue(getLastQueryStringFor(type))
    }
  }

  var switchToGizmo = function () {
    $("#selected-query-lang").html("Gizmo " + caretSpan)
    selectedQueryLanguage = "gizmo"
  }

  var switchToMQL = function() {
    $("#selected-query-lang").html("MQL" + caretSpan)
    selectedQueryLanguage = "mql"
  }

  var switchToGraphQL = function() {
    $("#selected-query-lang").html("GraphQL" + caretSpan)
    selectedQueryLanguage = "graphql"
  }

  selectedQueryLanguage = "gizmo"
  var caretSpan = " &nbsp <span class='caret'></span>"

  if (typeof(Storage) !== "undefined") {
    savedQueries = localStorage.getItem("cayleySavedQueriesgraphql");
    if (savedQueries === null) {
      for (var key in defaultQueryStrings) {
        localStorage.setItem("cayleySavedQueries" + key, defaultQueryStrings[key])
      }
    }
    lang = localStorage.getItem("cayleyQueryLang");
    if (lang !== null) {
      switchTo(lang)
    } else {
      switchTo("gizmo")
    }
  } else {
    switchTo("gizmo")
  }


  $("#gizmo-dropdown").click(function() {
    switchTo("gizmo")
  })

  $("#mql-dropdown").click(function() {
    switchTo("mql")
  })

  $("#graphql-dropdown").click(function() {
    switchTo("graphql")
  })
});


