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
	query   string
	tag     string
	expect  []string
}{
	// Easy one to get us started. How quick is the most straightforward retrieval?
	{
		message: "name predicate",
		query: `
		g.V("Humphrey Bogart").In("name").All()
		`,
		expect: []string{":/en/humphrey_bogart"},
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
		expect: []string{},
	},

	// 2014-07-12: This one takes about 4 whole seconds in memory. This is a behemoth.
	{
		message: "three huge sets with small intersection",
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
		expect: []string{":/en/billy_gilbert", ":/en/sterling_holloway"},
	},

	// This is more of an optimization problem that will get better over time. This takes a lot
	// of wrong turns on the walk down to what is ultimately the name, but top AND has it easy
	// as it has a fixed ID. Exercises Check().
	{
		message: "the helpless checker",
		query: `
			g.V().As("person").In("name").In().In().Out("name").Is("Casablanca").All()
			`,
		tag: "person",
		expect: []string{
			"Claude Rains",
			"Conrad Veidt",
			"Dooley Wilson",
			"Helmut Dantine",
			"Humphrey Bogart",
			"Ingrid Bergman",
			"John Qualen",
			"Joy Page",
			"Leonid Kinskey",
			"Lou Marcelle",
			"Madeleine LeBeau",
			"Paul Henreid",
			"Peter Lorre",
			"Sydney Greenstreet",
			"S.Z. Sakall",
		},
	},

	//Q: Who starred in both "The Net" and "Speed" ?
	//A: "Sandra Bullock"
	{
		message: "Net and Speed",
		query: common + `m1_actors.Intersect(m2_actors).Out("name").All()
`,
	},

	//Q: Did "Keanu Reeves" star in "The Net" ?
	//A: No
	{
		message: "Keannu in The Net",
		query: common + `actor2.Intersect(m1_actors).Out("name").All()
`,
	},

	//Q: Did "Keanu Reeves" star in "Speed" ?
	//A: Yes
	{
		message: "Keannu in Speed",
		query: common + `actor2.Intersect(m2_actors).Out("name").All()
`,
	},

	//Q: Has "Keanu Reeves" co-starred with anyone who starred in "The Net" ?
	//A: "Keanu Reeves" was in "Speed" and "The Lake House" with "Sandra Bullock",
	//   who was in "The Net"
	{
		message: "Keannu with other in The Net",
		query: common + `actor2.Follow(coStars1).Intersect(m1_actors).Out("name").All()
`,
	},

	//Q5: Do "Keanu Reeves" and "Sandra Bullock" have any commons co-stars?
	//A5: Yes, many. For example: SB starred with "Steve Martin" in "The Prince
	//    of Egypt", and KR starred with Steven Martin in "Parenthood".
	{
		message: "Keannu and Bullock with other",
		query: common + `actor1.Save("name","costar1_actor").Follow(coStars1).Intersect(actor2.Save("name","costar2_actor").Follow(coStars2)).Out("name").All()
`,
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
		DatabasePath:   "30kmoviedata.nt.gz",
		DatabaseType:   "memstore",
		GremlinTimeout: 1,
	}

	ts graph.TripleStore
)

func runBench(n int, b *testing.B) {
	var err error
	once.Do(func() {
		ts, err = db.Open(cfg)
		if err != nil {
			b.Fatalf("Failed to open %q: %v", cfg.DatabasePath, err)
		}
	})
	ses := gremlin.NewSession(ts, cfg.GremlinTimeout, true)
	_, err = ses.InputParses(benchmarkQueries[n].query)
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

func BenchmarkHelplessChecker(b *testing.B) {
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
