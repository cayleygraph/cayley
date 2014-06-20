# Copyright 2014 The Cayley Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import svgwrite
import math
import random

dwg = svgwrite.Drawing((210,130))


node_radius = 15
big_radius = 50
fan_dist = 90
center = (105,65)
edge_stroke = 2.5
edge_color = '#555555'
fan_color = '#aa0000'

b = '#4285F4'
r = '#DB4437'
g = '#0F9D58'
y = '#F4B400'
n = '#999999'

center_colors = [b, r, y, b, r, y]
edge_color = n
fan_color = n

def center_list(center, big_r):
    x, y = center
    out = []
    for i in range(0,6):
        ox = x + (math.cos(2 * math.pi * i / 6.0) * big_r)
        oy = y - (math.sin(2 * math.pi * i / 6.0) * big_r)
        out.append((ox, oy))
    return out

cx, cy = center
ring_centers = center_list(center, big_radius)
outer_left = (cx - fan_dist, cy)
outer_right = (cx + fan_dist, cy)
left = ring_centers[3]
right = ring_centers[0]

all_lines = []
l = dwg.add(dwg.line(outer_left, left))
l.stroke(edge_color, edge_stroke)
all_lines.append(l)
l = dwg.add(dwg.line(outer_right, right))
l.stroke(edge_color, edge_stroke)
all_lines.append(l)

for i, c in enumerate(ring_centers):
    for j, d in enumerate(ring_centers):
        if i > j or i == j:
            continue
        if (i % 3) == (j % 3):
            continue
        if (i % 3) == 1 and (j % 3) == 2:
            continue
        if (j % 3) == 1 and (i % 3) == 2:
            continue
        if i == 0 and j == 3:
            continue
        if i == 3 and j == 0:
            continue
        l = dwg.add(dwg.line(c,d))
        l.stroke(edge_color, edge_stroke)
        all_lines.append(l)

circle_elems = []
for i, c in enumerate(ring_centers):
    elem = dwg.add(dwg.circle(c, node_radius, fill=center_colors[i]))
    circle_elems.append(elem)

left_circle = dwg.add(dwg.circle(outer_left, node_radius, fill=fan_color))
right_circle = dwg.add(dwg.circle(outer_right, node_radius, fill=fan_color))


anims = []
def flash(element, orig_color, start, is_line=False):
    prop = "fill"
    if is_line:
        prop = "stroke"

    a = svgwrite.animate.Animate(prop, href=element)
    a['from'] = orig_color
    a['to'] = g
    a['begin'] = "+%0.2fs" % start
    a['dur'] = "1.0s"
    dwg.add(a)
    anims.append(a)

    a = svgwrite.animate.Animate(prop, href=element)
    a['from'] = g
    a['to'] = orig_color
    a['begin'] = "+%0.2fs" % (start + 1.0)
    a['dur'] = "1.2s"

    dwg.add(a)
    anims.append(a)
    return a

dwg.saveas("cayley.svg")
first = flash(left_circle, n, 0)
flash(all_lines[0], n, 0.5, True)
flash(all_lines[7], n, 1.0, True)
flash(all_lines[3], n, 1.5, True)
flash(all_lines[9], n, 1.0, True)
flash(all_lines[5], n, 1.5, True)
flash(all_lines[1], n, 2.0, True)
flash(right_circle, n, 2.5)
flash(left_circle, n, 3.5)
flash(all_lines[0], n, 4.0, True)
flash(all_lines[6], n, 4.5, True)
flash(all_lines[4], n, 5.0, True)
flash(all_lines[8], n, 4.5, True)
flash(all_lines[2], n, 5.0, True)
flash(all_lines[1], n, 5.5, True)
final = flash(right_circle, n, 6.0)

for anim in anims:
    anim["begin"] = anim["begin"] + "; " + final.get_id() + ".end" + anim["begin"]

dwg.saveas("cayley_active.svg")
