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

function Network() {
  var height = 800;
  var width = 800;

  // Things in the first force graph.
  var nodesG = null;
  var linksG = null;
  var linkNodesG = null;

  var node = null;
  var linknode = null;
  var link = null;

  var curLinksData = null;
  var curNodesData = null;


  // Things in the second force graph
  var tag_nodesG = null;
  var tag_linksG = null;

  var tag_node = null;
  var tag_link = null;

  var curTagLinksData = null;
  var curTagNodesData = null;


  var allData = null;

  var dragstart = function(d) {
    d3.select(this).classed("fixed", d.fixed = true);
  }

  //our force directed layout
  var force = d3.layout.force()
  var force_drag = force.drag().on("dragstart", dragstart)
  var tag_force = d3.layout.force()

  //color function used to color nodes
  var baseNodeColors = d3.scale.category20()
  var linkNodeColors = function(d) {
    return d3.rgb("#555").toString()
  }
  var strokeFor = function(d) {
    return d3.rgb("#000").brighter().toString()
  }

  var rotationTransformForLinkNode = function (d) {
    if (!d.link) {
      return null
    }
    var center_x = (d.link.source.x + d.link.target.x) / 2
    var center_y = (d.link.source.y + d.link.target.y) / 2
    var dx = d.link.target.x - d.link.source.x
    var dy = d.link.target.y - d.link.source.y
    var rotation_radians = Math.atan2(dy,dx)
    var rotation_degrees = rotation_radians * (180 / Math.PI)
    return "rotate(" + rotation_degrees + ", " + center_x + ", " + center_y + ")"
  }

  var updateLink = function() {
    this.attr("x1", function(d) {return d.source.x})
        .attr("y1", function(d) {return d.source.y})
        .attr("x2", function(d) {return d.target.x})
        .attr("y2", function(d) {return d.target.y})
  }

  var forceTick = function(e) {
    tag_force.start()
    node
      .attr("cx", function(d) {return d.x})
      .attr("cy", function(d) {return d.y})

    linknode.each(function(d) {
      if (d.link) {
        d.x = (d.link.source.x + d.link.target.x) / 2
        d.y = (d.link.source.y + d.link.target.y) / 2
      }
    })

    linknode
      .attr("cx", function(d) {return d.x })
      .attr("cy", function(d) {return d.y })
      .attr("transform", rotationTransformForLinkNode)

    if (link) {
      link.call(updateLink);
    }

    tagForceTick(e)
  }

  var tagForceTick = function(e) {
    tag_node.each(function(d) {
      if(d.is_tag === false) {
        d.x = d.node.x;
        d.y = d.node.y;
      } else {
        var b = this.childNodes[1].getBBox();
        var diffX = d.x - d.node.x;
        var diffY = d.y - d.node.y;

        var dist = Math.sqrt(diffX * diffX + diffY * diffY);

        var shiftX = b.width * (diffX - dist) / (dist * 2);
        shiftX = Math.max(-b.width, Math.min(0, shiftX));
        var shiftY = 5;
        this.childNodes[1].setAttribute("transform", "translate(" + shiftX + "," + shiftY + ")");
      }
    });
    tag_node
        .attr("transform", function(d) {
                return "translate(" + d.x + "," + d.y + ")";
        });

    tag_link.call(updateLink);
  }

  var setupData = function (data) {
    data.nodes.forEach(function (n) {
      n.x = randomnumber=Math.floor(Math.random()*width)
      n.y = randomnumber=Math.floor(Math.random()*height)
      n.radius = 10
    });

    var nodesMap = mapNodes(data.nodes)

    if (data.links) {
      data.links.forEach(function (l) {
        l.source = nodesMap.get(l.source)
        l.target = nodesMap.get(l.target)
        nodesMap.get(l.link_node).link = l
      })
    }

    data.tag_links = []
    data.tag_nodes = []
    var tag_id_counter = 0

    data.nodes.forEach(function (n) {
      if (n.tags !== undefined) {
        n.tags.forEach( function (tag) {
          var tag_node = {}
          tag_node.id = "tag" + tag_id_counter
          tag_node.tag = tag
          tag_node.x = n.x
          tag_node.y = n.y
          tag_node.is_tag = true
          tag_node.is_value = false
          tag_node.node = n
          tag_node.radius = 0
          tag_id_counter += 1;
          var fake_node = {}
          fake_node.id = "tag" + tag_id_counter
          fake_node.x = n.x
          fake_node.y = n.y
          fake_node.node = n
          fake_node.radius = 0
          fake_node.is_tag = false
          tag_id_counter += 1;
          var tag_link = {}
          tag_link.source = fake_node
          tag_link.target = tag_node
          data.tag_nodes.push(tag_node)
          data.tag_nodes.push(fake_node)
          data.tag_links.push(tag_link)
        })
      }
      if (n.values !== undefined) {
        n.values.forEach( function (value) {
          var tag_node = {}
          tag_node.id = "tag" + tag_id_counter
          tag_node.tag = value
          tag_node.x = n.x
          tag_node.y = n.y
          tag_node.is_tag = true
          tag_node.is_value = true
          tag_node.node = n
          tag_node.radius = 0
          tag_id_counter += 1;
          var fake_node = {}
          fake_node.id = "tag" + tag_id_counter
          fake_node.x = n.x
          fake_node.y = n.y
          fake_node.node = n
          fake_node.radius = 0
          fake_node.is_tag = false
          tag_id_counter += 1;
          var tag_link = {}
          tag_link.source = fake_node
          tag_link.target = tag_node
          data.tag_nodes.push(tag_node)
          data.tag_nodes.push(fake_node)
          data.tag_links.push(tag_link)
        })
      }
    })

    return data;
  }

  var mapNodes = function (nodes) {
    var nodesMap = d3.map()
    nodes.forEach(function (n) {
      nodesMap.set(n.id, n)
    })
    return nodesMap
  }

  var network = function (selection, data) {
    allData = setupData(data)
    var vis = d3.select(selection).append("svg")
       .attr("width", width)
       .attr("height", height)

    vis.append("defs").append("marker")
    .attr("id", "arrowhead")
    .attr("refX", 6 + 3) // shift?
    .attr("refY", 2)
    .attr("markerWidth", 6)
    .attr("markerHeight", 4)
    .attr("orient", "auto")
    .append("path")
    .attr("d", "M 0,0 V 4 L6,2 Z");

    linksG = vis.append("g").attr("id", "links")
    nodesG = vis.append("g").attr("id", "nodes")
    linkNodesG = vis.append("g").attr("id", "link-nodes")

    tagNodesG = vis.append("g").attr("id", "tag_nodes")
    tagLinksG = vis.append("g").attr("id", "tag_links")

    force.size([width, height])
    force.on("tick", forceTick)
      .charge(-200)
      .linkDistance(100);

    tag_force
      .gravity(0)
      .linkDistance(25)
      .linkStrength(20)
      .charge(-100)
      .size([width, height])

    tag_force.start()

    //perform rendering and start force layout
    update()

  }



  var update = function () {
    allNodes = allData.nodes;
    curLinksData = allData.links;
    curNodesData = $.grep(allData.nodes, function(n) { return n.is_link_node === false; })
    curLinkNodesData = $.grep(allData.nodes, function(n) { return n.is_link_node; })

    curTagLinksData = allData.tag_links
    curTagNodesData = allData.tag_nodes

    force.nodes(allNodes);
    node = nodesG.selectAll("circle.node")
      .data(curNodesData, function(d) {return d.id})

    node.enter().append("circle")
      .attr("class", "node")
      .attr("cx", function(d) {return d.x})
      .attr("cy", function(d) {return d.y})
      .attr("r", function(d) {return d.radius;})
      .style("fill", function(d) {return baseNodeColors(d.id);})
      .style("stroke", function(d) {return strokeFor(d);})
      .style("stroke-width", 1.0)
      .call(force_drag)

    node.exit().remove();

    linknode = linkNodesG.selectAll("ellipse.node")
      .data(curLinkNodesData, function(d) { return d.id })

    linknode.enter().append("ellipse")
      .attr("class", "node")
      .attr("cx", function(d) {return d.x})
      .attr("cy", function(d) {return d.y})
      .attr("rx", function(d) {return d.radius;})
      .attr("ry", function(d) {return d.radius / 2;})
      .style("fill", function(d) {return linkNodeColors(d.id);})
      .style("stroke", function(d) {return strokeFor(d);})
      .style("stroke-width", 1.0);

    linknode.exit().remove();


    if (curLinksData) {
      force.links(curLinksData);

      link = linksG.selectAll("line.link")
        .data(curLinksData, function(d) { return d.source.id + "_" + d.target.id});

      link.enter().append("line")
        .attr("class", "link")
        .attr("stroke", "#222")
        .attr("stroke-opacity", 1.0)
        .attr("marker-end", "url(#arrowhead)")
        .style("stroke-width", 2.0)
        .attr("x1", function(d) {return d.source.x})
        .attr("y1", function(d) {return d.source.y})
        .attr("x2", function(d) {return d.target.x})
        .attr("y2", function(d) {return d.target.y});

      link.exit().remove();
    }

    force.start();

    tag_force.nodes(curTagNodesData);
    tag_node = nodesG.selectAll("g.tag_node")
        .data(curTagNodesData, function(d) { return d.id })
    var tag_g = tag_node.enter().append("g").attr("class", "tag_node")
    tag_g.append("svg:circle").attr("r", 0).style("fill", "#FFF");
    tag_g.append("svg:text")
      .text(function(d) { return d.is_tag ? d.tag : "" })
      .style("fill", "#555")
      .style("font-family", function(d) { return d.is_value ? "Courier" : "Arial"})
      .style("font-size", 12);

    tag_force.links(curTagLinksData);
    tag_link = linksG.selectAll("line.tag_link")
      .data(curTagLinksData, function(d) { return d.source.id + "_" + d.target.id});

    tag_link.enter().append("line")
      .attr("class", "tag_link")
      .attr("stroke", "#ddd")
      .attr("stroke-opacity", 0.5)
      .attr("x1", function(d) {return d.source.x})
      .attr("y1", function(d) {return d.source.y})
      .attr("x2", function(d) {return d.target.x})
      .attr("y2", function(d) {return d.target.y});

    tag_link.exit().remove();
    tag_force.start()
  }

  return network
};

