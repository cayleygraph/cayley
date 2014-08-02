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

package main

import (
	"sync"
	"testing"

	"github.com/google/cayley/config"
	"github.com/google/cayley/db"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/query/gremlin"
)

var benchmarkQueries = []struct {
	message string
	long    bool
	query   string
	tag     string
	expect  [][]interface{}
}{
	// Easy one to get us started. How quick is the most straightforward retrieval?
	{
		message: "name predicate",
		query: `
		g.V("Humphrey Bogart").In("name").All()
		`,
		expect: [][]interface{}{
			{map[string]string{"id": "/en/humphrey_bogart"}},
		},
	},

	// Grunty queries.
	// 2014-07-12: This one seems to return in ~20ms in memory;
	// that's going to be measurably slower for every other backend.
	{
		message: "two large sets with no intersection",
		query: `
		function getId(x) { return g.V(x).In("name") }
		var actor_to_film = g.M().In("/film/performance/actor").In("/film/film/starring")

		getId("Oliver Hardy").Follow(actor_to_film).Out("name").Intersect(
			getId("Mel Blanc").Follow(actor_to_film).Out("name")).All()
			`,
		expect: nil,
	},

	// 2014-07-12: This one takes about 4 whole seconds in memory. This is a behemoth.
	{
		message: "three huge sets with small intersection",
		long:    true,
		query: `
			function getId(x) { return g.V(x).In("name") }
			var actor_to_film = g.M().In("/film/performance/actor").In("/film/film/starring")

			var a = getId("Oliver Hardy").Follow(actor_to_film).FollowR(actor_to_film)
			var b = getId("Mel Blanc").Follow(actor_to_film).FollowR(actor_to_film)
			var c = getId("Billy Gilbert").Follow(actor_to_film).FollowR(actor_to_film)

			seen = {}

			a.Intersect(b).Intersect(c).ForEach(function (d) {
				if (!(d.id in seen)) {
					seen[d.id] = true;
					g.Emit(d.id)
				}
			})
			`,
		expect: [][]interface{}{
			{"/en/billy_gilbert"},
			{"/en/sterling_holloway"},
		},
	},

	// This is more of an optimization problem that will get better over time. This takes a lot
	// of wrong turns on the walk down to what is ultimately the name, but top AND has it easy
	// as it has a fixed ID. Exercises Contains().
	{
		message: "the helpless checker",
		long:    true,
		query: `
			g.V().As("person").In("name").In().In().Out("name").Is("Casablanca").All()
			`,
		tag: "person",
		expect: [][]interface{}{
			{map[string]string{"id": "Casablanca", "person": "Claude Rains"}},
			{map[string]string{"id": "Casablanca", "person": "Conrad Veidt"}},
			{map[string]string{"id": "Casablanca", "person": "Dooley Wilson"}},
			{map[string]string{"id": "Casablanca", "person": "Helmut Dantine"}},
			{map[string]string{"id": "Casablanca", "person": "Humphrey Bogart"}},
			{map[string]string{"id": "Casablanca", "person": "Ingrid Bergman"}},
			{map[string]string{"id": "Casablanca", "person": "John Qualen"}},
			{map[string]string{"id": "Casablanca", "person": "Joy Page"}},
			{map[string]string{"id": "Casablanca", "person": "Leonid Kinskey"}},
			{map[string]string{"id": "Casablanca", "person": "Lou Marcelle"}},
			{map[string]string{"id": "Casablanca", "person": "Madeleine LeBeau"}},
			{map[string]string{"id": "Casablanca", "person": "Paul Henreid"}},
			{map[string]string{"id": "Casablanca", "person": "Peter Lorre"}},
			{map[string]string{"id": "Casablanca", "person": "Sydney Greenstreet"}},
			{map[string]string{"id": "Casablanca", "person": "S.Z. Sakall"}},
		},
	},

	//Q: Who starred in both "The Net" and "Speed" ?
	//A: "Sandra Bullock"
	{
		message: "Net and Speed",
		query: common + `m1_actors.Intersect(m2_actors).Out("name").All()
`,
		expect: [][]interface{}{
			{map[string]string{"id": "Sandra Bullock", "movie1": "The Net", "movie2": "Speed"}},
		},
	},

	//Q: Did "Keanu Reeves" star in "The Net" ?
	//A: No
	{
		message: "Keanu in The Net",
		query: common + `actor2.Intersect(m1_actors).Out("name").All()
`,
		expect: nil,
	},

	//Q: Did "Keanu Reeves" star in "Speed" ?
	//A: Yes
	{
		message: "Keanu in Speed",
		query: common + `actor2.Intersect(m2_actors).Out("name").All()
`,
		expect: [][]interface{}{
			{map[string]string{"id": "Keanu Reeves", "movie2": "Speed"}},
		},
	},

	//Q: Has "Keanu Reeves" co-starred with anyone who starred in "The Net" ?
	//A: "Keanu Reeves" was in "Speed" and "The Lake House" with "Sandra Bullock",
	//   who was in "The Net"
	{
		message: "Keanu with other in The Net",
		long:    true,
		query: common + `actor2.Follow(coStars1).Intersect(m1_actors).Out("name").All()
`,
		expect: [][]interface{}{
			{map[string]string{"id": "Sandra Bullock", "movie1": "The Net", "costar1_movie": "Speed"}},
			{map[string]string{"movie1": "The Net", "costar1_movie": "The Lake House", "id": "Sandra Bullock"}},
		},
	},

	//Q: Do "Keanu Reeves" and "Sandra Bullock" have any commons co-stars?
	//A: Yes, many. For example: SB starred with "Steve Martin" in "The Prince
	//    of Egypt", and KR starred with Steven Martin in "Parenthood".
	{
		message: "Keanu and Bullock with other",
		long:    true,
		query: common + `actor1.Save("name","costar1_actor").Follow(coStars1).Intersect(actor2.Save("name","costar2_actor").Follow(coStars2)).Out("name").All()
`,
		expect: [][]interface{}{
			{map[string]string{"costar2_movie": "Speed", "id": "Alan Ruck", "costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves"}},
			{map[string]string{"costar1_movie": "Demolition Man", "costar2_actor": "Keanu Reeves", "costar2_movie": "Thumbsucker", "id": "Benjamin Bratt", "costar1_actor": "Sandra Bullock"}},
			{map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Miss Congeniality", "costar2_actor": "Keanu Reeves", "costar2_movie": "Thumbsucker", "id": "Benjamin Bratt"}},
			{map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Beth Grant"}},
			{map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Carlos Carrasco"}},
			{map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Christopher Plummer"}},
			{map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Proposal", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Devil's Advocate", "id": "Craig T. Nelson"}},
			{map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "River's Edge", "id": "Dennis Hopper"}},
			{map[string]string{"costar2_movie": "Speed", "id": "Dennis Hopper", "costar1_actor": "/people/person", "costar1_movie": "Chattahoochee", "costar2_actor": "Keanu Reeves"}},
			{map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Practical Magic", "costar2_actor": "Keanu Reeves", "costar2_movie": "Parenthood", "id": "Dianne Wiest"}},
			{map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Dylan Walsh"}},
			{map[string]string{"costar2_movie": "Speed", "id": "Glenn Plummer", "costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves"}},
			{map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed 2: Cruise Control", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Glenn Plummer"}},
			{map[string]string{"costar1_movie": "While You Were Sleeping", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Replacements", "id": "Jack Warden", "costar1_actor": "Sandra Bullock"}},
			{map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Infamous", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Jeff Daniels"}},
			{map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Jeff Daniels"}},
			{map[string]string{"id": "Joe Morton", "costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed"}},
			{map[string]string{"costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Jordan Lund", "costar1_actor": "Sandra Bullock", "costar1_movie": "Speed"}},
			{map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Flying", "id": "Keanu Reeves"}},
			{map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "Flying", "id": "Keanu Reeves"}},
			{map[string]string{"costar1_movie": "The Day the Earth Stood Still", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Animatrix", "id": "Keanu Reeves", "costar1_actor": "/people/person"}},
			{map[string]string{"id": "Keanu Reeves", "costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "Tune in Tomorrow"}},
			{map[string]string{"costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Last Time I Committed Suicide", "id": "Keanu Reeves"}},
			{map[string]string{"costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "Constantine", "id": "Keanu Reeves"}},
			{map[string]string{"costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "Permanent Record", "id": "Keanu Reeves"}},
			{map[string]string{"costar2_movie": "Dangerous Liaisons", "id": "Keanu Reeves", "costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves"}},
			{map[string]string{"costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Private Lives of Pippa Lee", "id": "Keanu Reeves"}},
			{map[string]string{"costar2_movie": "A Scanner Darkly", "id": "Keanu Reeves", "costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves"}},
			{map[string]string{"costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "A Walk in the Clouds", "id": "Keanu Reeves"}},
			{map[string]string{"costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "Hardball", "id": "Keanu Reeves"}},
			{map[string]string{"costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "Life Under Water", "id": "Keanu Reeves"}},
			{map[string]string{"costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "Much Ado About Nothing", "id": "Keanu Reeves"}},
			{map[string]string{"costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "My Own Private Idaho", "id": "Keanu Reeves"}},
			{map[string]string{"costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "Parenthood", "id": "Keanu Reeves", "costar1_actor": "/people/person"}},
			{map[string]string{"costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "Point Break", "id": "Keanu Reeves", "costar1_actor": "/people/person"}},
			{map[string]string{"costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "Providence", "id": "Keanu Reeves"}},
			{map[string]string{"costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "River's Edge", "id": "Keanu Reeves"}},
			{map[string]string{"costar2_movie": "Something's Gotta Give", "id": "Keanu Reeves", "costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves"}},
			{map[string]string{"costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Keanu Reeves", "costar1_actor": "/people/person", "costar1_movie": "/film/film"}},
			{map[string]string{"costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "Sweet November", "id": "Keanu Reeves"}},
			{map[string]string{"costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Keanu Reeves", "costar1_actor": "/people/person"}},
			{map[string]string{"id": "Keanu Reeves", "costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Matrix Reloaded"}},
			{map[string]string{"costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Matrix Revisited", "id": "Keanu Reeves"}},
			{map[string]string{"costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Prince of Pennsylvania", "id": "Keanu Reeves"}},
			{map[string]string{"costar2_actor": "Keanu Reeves", "costar2_movie": "The Replacements", "id": "Keanu Reeves", "costar1_actor": "/people/person", "costar1_movie": "/film/film"}},
			{map[string]string{"costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "Even Cowgirls Get the Blues", "id": "Keanu Reeves"}},
			{map[string]string{"costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "Youngblood", "id": "Keanu Reeves"}},
			{map[string]string{"costar2_movie": "Bill & Ted's Bogus Journey", "id": "Keanu Reeves", "costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves"}},
			{map[string]string{"costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "Bill & Ted's Excellent Adventure", "id": "Keanu Reeves"}},
			{map[string]string{"id": "Keanu Reeves", "costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "Johnny Mnemonic"}},
			{map[string]string{"costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Devil's Advocate", "id": "Keanu Reeves", "costar1_actor": "/people/person"}},
			{map[string]string{"costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "Thumbsucker", "id": "Keanu Reeves"}},
			{map[string]string{"costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "I Love You to Death", "id": "Keanu Reeves"}},
			{map[string]string{"costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "Bram Stoker's Dracula", "id": "Keanu Reeves", "costar1_actor": "/people/person"}},
			{map[string]string{"costar2_actor": "Keanu Reeves", "costar2_movie": "The Gift", "id": "Keanu Reeves", "costar1_actor": "/people/person", "costar1_movie": "/film/film"}},
			{map[string]string{"costar2_movie": "Little Buddha", "id": "Keanu Reeves", "costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves"}},
			{map[string]string{"costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Night Watchman", "id": "Keanu Reeves"}},
			{map[string]string{"id": "Keanu Reeves", "costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "Chain Reaction"}},
			{map[string]string{"costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "Babes in Toyland", "id": "Keanu Reeves"}},
			{map[string]string{"costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Day the Earth Stood Still", "id": "Keanu Reeves"}},
			{map[string]string{"costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Lynn Collins", "costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House"}},
			{map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Proposal", "costar2_actor": "Keanu Reeves", "costar2_movie": "Parenthood", "id": "Mary Steenburgen"}},
			{map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Prince of Egypt", "costar2_actor": "Keanu Reeves", "costar2_movie": "Dangerous Liaisons", "id": "Michelle Pfeiffer"}},
			{map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Premonition", "costar2_actor": "Keanu Reeves", "costar2_movie": "Constantine", "id": "Peter Stormare"}},
			{map[string]string{"costar2_movie": "Speed", "id": "Richard Lineback", "costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves"}},
			{map[string]string{"costar1_movie": "The Thing Called Love", "costar2_actor": "Keanu Reeves", "costar2_movie": "My Own Private Idaho", "id": "River Phoenix", "costar1_actor": "Sandra Bullock"}},
			{map[string]string{"costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "I Love You to Death", "id": "River Phoenix"}},
			{map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Proposal", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"}},
			{map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Crash", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"}},
			{map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Gun Shy", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"}},
			{map[string]string{"costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock", "costar1_actor": "Sandra Bullock", "costar1_movie": "Demolition Man"}},
			{map[string]string{"costar2_movie": "Speed", "id": "Sandra Bullock", "costar1_actor": "Sandra Bullock", "costar1_movie": "Divine Secrets of the Ya-Ya Sisterhood", "costar2_actor": "Keanu Reeves"}},
			{map[string]string{"costar2_movie": "Speed", "id": "Sandra Bullock", "costar1_actor": "Sandra Bullock", "costar1_movie": "A Time to Kill", "costar2_actor": "Keanu Reeves"}},
			{map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Forces of Nature", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"}},
			{map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Hope Floats", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"}},
			{map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Infamous", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"}},
			{map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Love Potion No. 9", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"}},
			{map[string]string{"id": "Sandra Bullock", "costar1_actor": "Sandra Bullock", "costar1_movie": "Miss Congeniality", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed"}},
			{map[string]string{"id": "Sandra Bullock", "costar1_actor": "Sandra Bullock", "costar1_movie": "Miss Congeniality 2: Armed and Fabulous", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed"}},
			{map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Murder by Numbers", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"}},
			{map[string]string{"id": "Sandra Bullock", "costar1_actor": "Sandra Bullock", "costar1_movie": "Practical Magic", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed"}},
			{map[string]string{"costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock", "costar1_actor": "Sandra Bullock", "costar1_movie": "Speed"}},
			{map[string]string{"costar1_movie": "Speed 2: Cruise Control", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock", "costar1_actor": "Sandra Bullock"}},
			{map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"}},
			{map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Net", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"}},
			{map[string]string{"costar1_movie": "The Prince of Egypt", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock", "costar1_actor": "Sandra Bullock"}},
			{map[string]string{"costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock", "costar1_actor": "Sandra Bullock", "costar1_movie": "Two Weeks Notice"}},
			{map[string]string{"costar2_movie": "Speed", "id": "Sandra Bullock", "costar1_actor": "Sandra Bullock", "costar1_movie": "While You Were Sleeping", "costar2_actor": "Keanu Reeves"}},
			{map[string]string{"id": "Sandra Bullock", "costar1_actor": "Sandra Bullock", "costar1_movie": "28 Days", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed"}},
			{map[string]string{"costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock", "costar1_actor": "Sandra Bullock", "costar1_movie": "Premonition"}},
			{map[string]string{"costar1_movie": "Wrestling Ernest Hemingway", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock", "costar1_actor": "Sandra Bullock"}},
			{map[string]string{"costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock", "costar1_actor": "Sandra Bullock", "costar1_movie": "Fire on the Amazon"}},
			{map[string]string{"costar1_movie": "The Thing Called Love", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock", "costar1_actor": "Sandra Bullock"}},
			{map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "In Love and War", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"}},
			{map[string]string{"costar1_actor": "/people/person", "costar1_movie": "/film/film", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Sandra Bullock"}},
			{map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Divine Secrets of the Ya-Ya Sisterhood", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Private Lives of Pippa Lee", "id": "Shirley Knight"}},
			{map[string]string{"costar2_movie": "The Lake House", "id": "Shohreh Aghdashloo", "costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves"}},
			{map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Prince of Egypt", "costar2_actor": "Keanu Reeves", "costar2_movie": "Parenthood", "id": "Steve Martin"}},
			{map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Thomas Rosales, Jr."}},
			{map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Hawthorne James"}},
		},
	},
}

const common = `
var movie1 = g.V().Has("name", "The Net")
var movie2 = g.V().Has("name", "Speed")
var actor1 = g.V().Has("name", "Sandra Bullock")
var actor2 = g.V().Has("name", "Keanu Reeves")

// (film) -> starring -> (actor)
var filmToActor = g.Morphism().Out("/film/film/starring").Out("/film/performance/actor")

// (actor) -> starring -> [film -> starring -> (actor)]
var coStars1 = g.Morphism().In("/film/performance/actor").In("/film/film/starring").Save("name","costar1_movie").Follow(filmToActor)
var coStars2 = g.Morphism().In("/film/performance/actor").In("/film/film/starring").Save("name","costar2_movie").Follow(filmToActor)

// Stars for the movies "The Net" and "Speed"
var m1_actors = movie1.Save("name","movie1").Follow(filmToActor)
var m2_actors = movie2.Save("name","movie2").Follow(filmToActor)
`

var (
	once sync.Once
	cfg  = &config.Config{
		DatabasePath:   "30kmoviedata.nq.gz",
		DatabaseType:   "memstore",
		GremlinTimeout: 300,
	}

	ts graph.TripleStore
)

func prepare(t testing.TB) {
	var err error
	once.Do(func() {
		ts, err = db.Open(cfg)
		if err != nil {
			t.Fatalf("Failed to open %q: %v", cfg.DatabasePath, err)
		}
	})
}

func TestQueries(t *testing.T) {
	prepare(t)
	for _, test := range benchmarkQueries {
		if testing.Short() && test.long {
			continue
		}
		ses := gremlin.NewSession(ts, cfg.GremlinTimeout, true)
		_, err := ses.InputParses(test.query)
		if err != nil {
			t.Fatalf("Failed to parse benchmark gremlin %s: %v", test.message, err)
		}
		c := make(chan interface{}, 5)
		go ses.ExecInput(test.query, c, 100)
		var (
			got      [][]interface{}
			timedOut bool
		)
		for r := range c {
			ses.BuildJson(r)
			j, err := ses.GetJson()
			if j == nil && err == nil {
				continue
			}
			if err == gremlin.ErrKillTimeout {
				timedOut = true
				continue
			}
			got = append(got, j)
		}

		if timedOut {
			t.Error("Query timed out: skipping validation.")
			continue
		}

		// TODO(kortschak) Be more rigorous in this result validation.
		if len(got) != len(test.expect) {
			t.Errorf("Unexpected number of results, got:%d expect:%d.", len(got), len(test.expect))
		}
	}
}

func runBench(n int, b *testing.B) {
	if testing.Short() && benchmarkQueries[n].long {
		b.Skip()
	}
	prepare(b)
	ses := gremlin.NewSession(ts, cfg.GremlinTimeout, true)
	_, err := ses.InputParses(benchmarkQueries[n].query)
	if err != nil {
		b.Fatalf("Failed to parse benchmark gremlin %s: %v", benchmarkQueries[n].message, err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := make(chan interface{}, 5)
		go ses.ExecInput(benchmarkQueries[n].query, c, 100)
		for _ = range c {
		}
	}
}

func BenchmarkNamePredicate(b *testing.B) {
	runBench(0, b)
}

func BenchmarkLargeSetsNoIntersection(b *testing.B) {
	runBench(1, b)
}

func BenchmarkVeryLargeSetsSmallIntersection(b *testing.B) {
	runBench(2, b)
}

func BenchmarkHelplessContainsChecker(b *testing.B) {
	runBench(3, b)
}

func BenchmarkNetAndSpeed(b *testing.B) {
	runBench(4, b)
}

func BenchmarkKeannuAndNet(b *testing.B) {
	runBench(5, b)
}

func BenchmarkKeannuAndSpeed(b *testing.B) {
	runBench(6, b)
}

func BenchmarkKeannuOther(b *testing.B) {
	runBench(7, b)
}

func BenchmarkKeannuBullockOther(b *testing.B) {
	runBench(8, b)
}
