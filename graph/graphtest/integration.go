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

package graphtest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/graphtest/testutil"
	"github.com/cayleygraph/cayley/internal"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/gizmo"
	_ "github.com/cayleygraph/cayley/writer"
)

const (
	format  = "nquads"
	timeout = 300 * time.Second
)

const (
	nSpeed = "Speed"
	nLakeH = "The Lake House"

	SandraB = "Sandra Bullock"
	KeanuR  = "Keanu Reeves"
)

func checkIntegration(t testing.TB, force bool) {
	if testing.Short() {
		t.SkipNow()
	}
	if !force && os.Getenv("RUN_INTEGRATION") != "true" {
		t.Skip("skipping integration tests; set RUN_INTEGRATION=true to run them")
	}
}

func TestIntegration(t *testing.T, gen testutil.DatabaseFunc, force bool) {
	checkIntegration(t, force)
	qs, closer := prepare(t, gen)
	defer closer()

	checkQueries(t, qs, timeout)
}

func BenchmarkIntegration(t *testing.B, gen testutil.DatabaseFunc, force bool) {
	checkIntegration(t, force)
	benchmarkQueries(t, gen)
}

func costarTag(id, c1, c1m, c2, c2m string) map[string]string {
	return map[string]string{
		"id":            id,
		"costar1_actor": c1,
		"costar1_movie": c1m,
		"costar2_actor": c2,
		"costar2_movie": c2m,
	}
}

var queries = []struct {
	message string
	long    bool
	query   string
	tag     string
	// for testing
	skip   bool
	expect []interface{}
}{
	// Easy one to get us started. How quick is the most straightforward retrieval?
	{
		message: "name predicate",
		query: `
		g.V("Humphrey Bogart").in("<name>").all()
		`,
		expect: []interface{}{
			map[string]string{"id": "</en/humphrey_bogart>"},
		},
	},

	// Grunty queries.
	// 2014-07-12: This one seems to return in ~20ms in memory;
	// that's going to be measurably slower for every other backend.
	{
		message: "two large sets with no intersection",
		query: `
		function getId(x) { return g.V(x).in("<name>") }
		var actor_to_film = g.M().in("</film/performance/actor>").in("</film/film/starring>")

		getId("Oliver Hardy").follow(actor_to_film).out("<name>").intersect(
			getId("Mel Blanc").follow(actor_to_film).out("<name>")).all()
			`,
		expect: nil,
	},

	// 2014-07-12: This one takes about 4 whole seconds in memory. This is a behemoth.
	{
		message: "three huge sets with small intersection",
		long:    true,
		query: `
			function getId(x) { return g.V(x).in("<name>") }
			var actor_to_film = g.M().in("</film/performance/actor>").in("</film/film/starring>")

			var a = getId("Oliver Hardy").follow(actor_to_film).followR(actor_to_film)
			var b = getId("Mel Blanc").follow(actor_to_film).followR(actor_to_film)
			var c = getId("Billy Gilbert").follow(actor_to_film).followR(actor_to_film)

			seen = {}

			a.intersect(b).intersect(c).forEach(function (d) {
				if (!(d.id in seen)) {
					seen[d.id] = true;
					g.emit(d)
				}
			})
			`,
		expect: []interface{}{
			map[string]string{"id": "</en/sterling_holloway>"},
			map[string]string{"id": "</en/billy_gilbert>"},
		},
	},

	// This is more of an optimization problem that will get better over time. This takes a lot
	// of wrong turns on the walk down to what is ultimately the name, but top AND has it easy
	// as it has a fixed ID. Exercises Contains().
	{
		message: "the helpless checker",
		long:    true,
		query: `
			g.V().as("person").in("<name>").in().in().out("<name>").is("Casablanca").all()
			`,
		tag: "person",
		expect: []interface{}{
			map[string]string{"id": "Casablanca", "person": "Ingrid Bergman"},
			map[string]string{"id": "Casablanca", "person": "Madeleine LeBeau"},
			map[string]string{"id": "Casablanca", "person": "Joy Page"},
			map[string]string{"id": "Casablanca", "person": "Claude Rains"},
			map[string]string{"id": "Casablanca", "person": "S.Z. Sakall"},
			map[string]string{"id": "Casablanca", "person": "Helmut Dantine"},
			map[string]string{"id": "Casablanca", "person": "Conrad Veidt"},
			map[string]string{"id": "Casablanca", "person": "Paul Henreid"},
			map[string]string{"id": "Casablanca", "person": "Peter Lorre"},
			map[string]string{"id": "Casablanca", "person": "Sydney Greenstreet"},
			map[string]string{"id": "Casablanca", "person": "Leonid Kinskey"},
			map[string]string{"id": "Casablanca", "person": "Lou Marcelle"},
			map[string]string{"id": "Casablanca", "person": "Dooley Wilson"},
			map[string]string{"id": "Casablanca", "person": "John Qualen"},
			map[string]string{"id": "Casablanca", "person": "Humphrey Bogart"},
		},
	},

	// Exercises Not().Contains(), as above.
	{
		message: "the helpless checker, negated (films without Ingrid Bergman)",
		long:    true,
		query: `
			g.V().as("person").in("<name>").in().in().out("<name>").except(g.V("Ingrid Bergman").in("<name>").in().in().out("<name>")).is("Casablanca").all()
			`,
		tag:    "person",
		expect: nil,
	},
	{
		message: "the helpless checker, negated (without actors Ingrid Bergman)",
		long:    true,
		query: `
			g.V().as("person").in("<name>").except(g.V("Ingrid Bergman").in("<name>")).in().in().out("<name>").is("Casablanca").all()
			`,
		tag: "person",
		expect: []interface{}{
			map[string]string{"id": "Casablanca", "person": "Madeleine LeBeau"},
			map[string]string{"id": "Casablanca", "person": "Joy Page"},
			map[string]string{"id": "Casablanca", "person": "Claude Rains"},
			map[string]string{"id": "Casablanca", "person": "S.Z. Sakall"},
			map[string]string{"id": "Casablanca", "person": "Helmut Dantine"},
			map[string]string{"id": "Casablanca", "person": "Conrad Veidt"},
			map[string]string{"id": "Casablanca", "person": "Paul Henreid"},
			map[string]string{"id": "Casablanca", "person": "Peter Lorre"},
			map[string]string{"id": "Casablanca", "person": "Sydney Greenstreet"},
			map[string]string{"id": "Casablanca", "person": "Leonid Kinskey"},
			map[string]string{"id": "Casablanca", "person": "Lou Marcelle"},
			map[string]string{"id": "Casablanca", "person": "Dooley Wilson"},
			map[string]string{"id": "Casablanca", "person": "John Qualen"},
			map[string]string{"id": "Casablanca", "person": "Humphrey Bogart"},
		},
	},

	//Q: Who starred in both "The Net" and "Speed" ?
	//A: "Sandra Bullock"
	{
		message: "Net and Speed",
		query: common + `m1_actors.intersect(m2_actors).out("<name>").all()
`,
		expect: []interface{}{
			map[string]string{"id": SandraB, "movie1": "The Net", "movie2": nSpeed},
		},
	},

	//Q: Did "Keanu Reeves" star in "The Net" ?
	//A: No
	{
		message: "Keanu in The Net",
		query: common + `actor2.intersect(m1_actors).out("<name>").all()
`,
		expect: nil,
	},

	//Q: Did "Keanu Reeves" star in "Speed" ?
	//A: Yes
	{
		message: "Keanu in Speed",
		query: common + `actor2.intersect(m2_actors).out("<name>").all()
`,
		expect: []interface{}{
			map[string]string{"id": KeanuR, "movie2": nSpeed},
		},
	},

	//Q: Has "Keanu Reeves" co-starred with anyone who starred in "The Net" ?
	//A: "Keanu Reeves" was in "Speed" and "The Lake House" with "Sandra Bullock",
	//   who was in "The Net"
	{
		message: "Keanu with other in The Net",
		long:    true,
		query: common + `actor2.follow(coStars1).intersect(m1_actors).out("<name>").all()
`,
		expect: []interface{}{
			map[string]string{"id": SandraB, "movie1": "The Net", "costar1_movie": nSpeed},
			map[string]string{"movie1": "The Net", "costar1_movie": nLakeH, "id": SandraB},
		},
	},

	//Q: Do "Keanu Reeves" and "Sandra Bullock" have any commons co-stars?
	//A: Yes, many. For example: SB starred with "Steve Martin" in "The Prince
	//    of Egypt", and KR starred with Steven Martin in "Parenthood".
	{
		message: "Keanu and Bullock with other",
		long:    true,
		query: common + `actor1.save("<name>","costar1_actor").follow(coStars1).intersect(actor2.save("<name>","costar2_actor").follow(coStars2)).out("<name>").all()
`,
		expect: []interface{}{
			costarTag(SandraB, SandraB, "The Proposal", KeanuR, nSpeed),
			costarTag(SandraB, SandraB, "The Proposal", KeanuR, nLakeH),
			costarTag("Mary Steenburgen", SandraB, "The Proposal", KeanuR, "Parenthood"),
			costarTag("Craig T. Nelson", SandraB, "The Proposal", KeanuR, "The Devil's Advocate"),
			costarTag(SandraB, SandraB, "Crash", KeanuR, nSpeed),
			costarTag(SandraB, SandraB, "Crash", KeanuR, nLakeH),
			costarTag(SandraB, SandraB, "Gun Shy", KeanuR, nSpeed),
			costarTag(SandraB, SandraB, "Gun Shy", KeanuR, nLakeH),
			costarTag(SandraB, SandraB, "Demolition Man", KeanuR, nSpeed),
			costarTag(SandraB, SandraB, "Demolition Man", KeanuR, nLakeH),
			costarTag("Benjamin Bratt", SandraB, "Demolition Man", KeanuR, "Thumbsucker"),
			costarTag(SandraB, SandraB, "Divine Secrets of the Ya-Ya Sisterhood", KeanuR, nSpeed),
			costarTag(SandraB, SandraB, "Divine Secrets of the Ya-Ya Sisterhood", KeanuR, nLakeH),
			costarTag("Shirley Knight", SandraB, "Divine Secrets of the Ya-Ya Sisterhood", KeanuR, "The Private Lives of Pippa Lee"),
			costarTag(SandraB, SandraB, "A Time to Kill", KeanuR, nSpeed),
			costarTag(SandraB, SandraB, "A Time to Kill", KeanuR, nLakeH),
			costarTag(SandraB, SandraB, "Forces of Nature", KeanuR, nSpeed),
			costarTag(SandraB, SandraB, "Forces of Nature", KeanuR, nLakeH),
			costarTag(SandraB, SandraB, "Hope Floats", KeanuR, nSpeed),
			costarTag(SandraB, SandraB, "Hope Floats", KeanuR, nLakeH),
			costarTag(SandraB, SandraB, "Infamous", KeanuR, nSpeed),
			costarTag(SandraB, SandraB, "Infamous", KeanuR, nLakeH),
			costarTag("Jeff Daniels", SandraB, "Infamous", KeanuR, nSpeed),
			costarTag(SandraB, SandraB, "Love Potion No. 9", KeanuR, nSpeed),
			costarTag(SandraB, SandraB, "Love Potion No. 9", KeanuR, nLakeH),
			costarTag(SandraB, SandraB, "Miss Congeniality", KeanuR, nSpeed),
			costarTag(SandraB, SandraB, "Miss Congeniality", KeanuR, nLakeH),
			costarTag("Benjamin Bratt", SandraB, "Miss Congeniality", KeanuR, "Thumbsucker"),
			costarTag(SandraB, SandraB, "Miss Congeniality 2: Armed and Fabulous", KeanuR, nSpeed),
			costarTag(SandraB, SandraB, "Miss Congeniality 2: Armed and Fabulous", KeanuR, nLakeH),
			costarTag(SandraB, SandraB, "Murder by Numbers", KeanuR, nSpeed),
			costarTag(SandraB, SandraB, "Murder by Numbers", KeanuR, nLakeH),
			costarTag(SandraB, SandraB, "Practical Magic", KeanuR, nSpeed),
			costarTag(SandraB, SandraB, "Practical Magic", KeanuR, nLakeH),
			costarTag("Dianne Wiest", SandraB, "Practical Magic", KeanuR, "Parenthood"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "Flying"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "The Animatrix"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "Tune in Tomorrow"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "The Last Time I Committed Suicide"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "Constantine"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "Permanent Record"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "Dangerous Liaisons"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "The Private Lives of Pippa Lee"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "A Scanner Darkly"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "A Walk in the Clouds"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "Hardball"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "Life Under Water"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "Much Ado About Nothing"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "My Own Private Idaho"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "Parenthood"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "Point Break"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "Providence"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "River's Edge"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "Something's Gotta Give"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, nSpeed),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "Sweet November"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, nLakeH),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "The Matrix Reloaded"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "The Matrix Revisited"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "The Prince of Pennsylvania"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "The Replacements"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "Even Cowgirls Get the Blues"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "Youngblood"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "Bill & Ted's Bogus Journey"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "Bill & Ted's Excellent Adventure"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "Johnny Mnemonic"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "The Devil's Advocate"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "Thumbsucker"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "I Love You to Death"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "Bram Stoker's Dracula"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "The Gift"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "Little Buddha"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "The Night Watchman"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "Chain Reaction"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "Babes in Toyland"),
			costarTag(KeanuR, SandraB, nSpeed, KeanuR, "The Day the Earth Stood Still"),
			costarTag(SandraB, SandraB, nSpeed, KeanuR, nSpeed),
			costarTag(SandraB, SandraB, nSpeed, KeanuR, nLakeH),
			costarTag("Dennis Hopper", SandraB, nSpeed, KeanuR, "River's Edge"),
			costarTag("Dennis Hopper", SandraB, nSpeed, KeanuR, nSpeed),
			costarTag("Jeff Daniels", SandraB, nSpeed, KeanuR, nSpeed),
			costarTag("Joe Morton", SandraB, nSpeed, KeanuR, nSpeed),
			costarTag("Alan Ruck", SandraB, nSpeed, KeanuR, nSpeed),
			costarTag("Glenn Plummer", SandraB, nSpeed, KeanuR, nSpeed),
			costarTag("Carlos Carrasco", SandraB, nSpeed, KeanuR, nSpeed),
			costarTag("Beth Grant", SandraB, nSpeed, KeanuR, nSpeed),
			costarTag("Richard Lineback", SandraB, nSpeed, KeanuR, nSpeed),
			costarTag("Hawthorne James", SandraB, nSpeed, KeanuR, nSpeed),
			costarTag("Jordan Lund", SandraB, nSpeed, KeanuR, nSpeed),
			costarTag("Thomas Rosales, Jr.", SandraB, nSpeed, KeanuR, nSpeed),
			costarTag(SandraB, SandraB, "Speed 2: Cruise Control", KeanuR, nSpeed),
			costarTag(SandraB, SandraB, "Speed 2: Cruise Control", KeanuR, nLakeH),
			costarTag("Glenn Plummer", SandraB, "Speed 2: Cruise Control", KeanuR, nSpeed),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "Flying"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "The Animatrix"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "Tune in Tomorrow"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "The Last Time I Committed Suicide"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "Constantine"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "Permanent Record"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "Dangerous Liaisons"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "The Private Lives of Pippa Lee"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "A Scanner Darkly"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "A Walk in the Clouds"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "Hardball"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "Life Under Water"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "Much Ado About Nothing"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "My Own Private Idaho"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "Parenthood"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "Point Break"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "Providence"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "River's Edge"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "Something's Gotta Give"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, nSpeed),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "Sweet November"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, nLakeH),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "The Matrix Reloaded"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "The Matrix Revisited"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "The Prince of Pennsylvania"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "The Replacements"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "Even Cowgirls Get the Blues"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "Youngblood"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "Bill & Ted's Bogus Journey"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "Bill & Ted's Excellent Adventure"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "Johnny Mnemonic"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "The Devil's Advocate"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "Thumbsucker"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "I Love You to Death"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "Bram Stoker's Dracula"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "The Gift"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "Little Buddha"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "The Night Watchman"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "Chain Reaction"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "Babes in Toyland"),
			costarTag(KeanuR, SandraB, nLakeH, KeanuR, "The Day the Earth Stood Still"),
			costarTag(SandraB, SandraB, nLakeH, KeanuR, nSpeed),
			costarTag(SandraB, SandraB, nLakeH, KeanuR, nLakeH),
			costarTag("Christopher Plummer", SandraB, nLakeH, KeanuR, nLakeH),
			costarTag("Dylan Walsh", SandraB, nLakeH, KeanuR, nLakeH),
			costarTag("Shohreh Aghdashloo", SandraB, nLakeH, KeanuR, nLakeH),
			costarTag("Lynn Collins", SandraB, nLakeH, KeanuR, nLakeH),
			costarTag(SandraB, SandraB, "The Net", KeanuR, nSpeed),
			costarTag(SandraB, SandraB, "The Net", KeanuR, nLakeH),
			costarTag("Michelle Pfeiffer", SandraB, "The Prince of Egypt", KeanuR, "Dangerous Liaisons"),
			costarTag(SandraB, SandraB, "The Prince of Egypt", KeanuR, nSpeed),
			costarTag(SandraB, SandraB, "The Prince of Egypt", KeanuR, nLakeH),
			costarTag("Steve Martin", SandraB, "The Prince of Egypt", KeanuR, "Parenthood"),
			costarTag(SandraB, SandraB, "Two Weeks Notice", KeanuR, nSpeed),
			costarTag(SandraB, SandraB, "Two Weeks Notice", KeanuR, nLakeH),
			costarTag(SandraB, SandraB, "While You Were Sleeping", KeanuR, nSpeed),
			costarTag(SandraB, SandraB, "While You Were Sleeping", KeanuR, nLakeH),
			costarTag("Jack Warden", SandraB, "While You Were Sleeping", KeanuR, "The Replacements"),
			costarTag(SandraB, SandraB, "28 Days", KeanuR, nSpeed),
			costarTag(SandraB, SandraB, "28 Days", KeanuR, nLakeH),
			costarTag(SandraB, SandraB, "Premonition", KeanuR, nSpeed),
			costarTag(SandraB, SandraB, "Premonition", KeanuR, nLakeH),
			costarTag("Peter Stormare", SandraB, "Premonition", KeanuR, "Constantine"),
			costarTag(SandraB, SandraB, "Wrestling Ernest Hemingway", KeanuR, nSpeed),
			costarTag(SandraB, SandraB, "Wrestling Ernest Hemingway", KeanuR, nLakeH),
			costarTag(SandraB, SandraB, "Fire on the Amazon", KeanuR, nSpeed),
			costarTag(SandraB, SandraB, "Fire on the Amazon", KeanuR, nLakeH),
			costarTag("River Phoenix", SandraB, "The Thing Called Love", KeanuR, "My Own Private Idaho"),
			costarTag("River Phoenix", SandraB, "The Thing Called Love", KeanuR, "I Love You to Death"),
			costarTag(SandraB, SandraB, "The Thing Called Love", KeanuR, nSpeed),
			costarTag(SandraB, SandraB, "The Thing Called Love", KeanuR, nLakeH),
			costarTag(SandraB, SandraB, "In Love and War", KeanuR, nSpeed),
			costarTag(SandraB, SandraB, "In Love and War", KeanuR, nLakeH),
		},
	},
	{
		message: "Save a number of predicates around a set of nodes",
		query: `
		g.V("_:9037", "_:49278", "_:44112", "_:44709", "_:43382").save("</film/performance/character>", "char").save("</film/performance/actor>", "act").saveR("</film/film/starring>", "film").all()
		`,
		expect: []interface{}{
			map[string]string{"act": "</en/humphrey_bogart>", "char": "Rick Blaine", "film": "</en/casablanca_1942>", "id": "_:9037"},
			map[string]string{"act": "</en/humphrey_bogart>", "char": "Sam Spade", "film": "</en/the_maltese_falcon_1941>", "id": "_:49278"},
			map[string]string{"act": "</en/humphrey_bogart>", "char": "Philip Marlowe", "film": "</en/the_big_sleep_1946>", "id": "_:44112"},
			map[string]string{"act": "</en/humphrey_bogart>", "char": "Captain Queeg", "film": "</en/the_caine_mutiny_1954>", "id": "_:44709"},
			map[string]string{"act": "</en/humphrey_bogart>", "char": "Charlie Allnut", "film": "</en/the_african_queen>", "id": "_:43382"},
		},
	},
}

const common = `
var movie1 = g.V().has("<name>", "The Net")
var movie2 = g.V().has("<name>", "Speed")
var actor1 = g.V().has("<name>", "Sandra Bullock")
var actor2 = g.V().has("<name>", "Keanu Reeves")

// (film) -> starring -> (actor)
var filmToActor = g.Morphism().out("</film/film/starring>").out("</film/performance/actor>")

// (actor) -> starring -> [film -> starring -> (actor)]
var coStars1 = g.Morphism().in("</film/performance/actor>").in("</film/film/starring>").save("<name>","costar1_movie").follow(filmToActor)
var coStars2 = g.Morphism().in("</film/performance/actor>").in("</film/film/starring>").save("<name>","costar2_movie").follow(filmToActor)

// Stars for the movies "The Net" and "Speed"
var m1_actors = movie1.save("<name>","movie1").follow(filmToActor)
var m2_actors = movie2.save("<name>","movie2").follow(filmToActor)
`

func prepare(t testing.TB, gen testutil.DatabaseFunc) (graph.QuadStore, func()) {
	qs, _, closer := gen(t)

	const needsLoad = true // TODO: support local setup
	if needsLoad {
		qw, err := qs.NewQuadWriter()
		if err != nil {
			closer()
			require.NoError(t, err)
		}

		start := time.Now()
		for _, p := range []string{"./", "../"} {
			err = internal.Load(qw, 0, filepath.Join(p, "../../data/30kmoviedata.nq.gz"), format)
			if err == nil || !os.IsNotExist(err) {
				break
			}
		}
		if err != nil {
			qw.Close()
			closer()
			require.NoError(t, err)
		}
		err = qw.Close()
		if err != nil {
			closer()
			require.NoError(t, err)
		}
		t.Logf("loaded data in %v", time.Since(start))
	}
	return qs, closer
}

func checkQueries(t *testing.T, qs graph.QuadStore, timeout time.Duration) {
	if qs == nil {
		t.Fatal("not initialized")
	}
	for _, test := range queries {
		t.Run(test.message, func(t *testing.T) {
			if testing.Short() && test.long {
				t.SkipNow()
			}
			if test.skip {
				t.SkipNow()
			}
			start := time.Now()
			ses := gizmo.NewSession(qs)
			ctx := context.Background()
			if timeout > 0 {
				var cancel func()
				ctx, cancel = context.WithTimeout(ctx, timeout)
				defer cancel()
			}
			it, err := ses.Execute(ctx, test.query, query.Options{
				Collation: query.JSON,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer it.Close()
			var got []interface{}
			for it.Next(ctx) {
				got = append(got, it.Result())
			}
			t.Logf("%12v %v", time.Since(start), test.message)

			if len(got) != len(test.expect) {
				t.Errorf("Unexpected number of results, got:%d expect:%d on %s.", len(got), len(test.expect), test.message)
				return
			}
			if unsortedEqual(got, test.expect) {
				return
			}
			t.Errorf("Unexpected results for %s:\n", test.message)
			for i := range got {
				t.Errorf("\n\tgot:%#v\n\texpect:%#v\n", got[i], test.expect[i])
			}
		})
	}
}

func unsortedEqual(got, expect []interface{}) bool {
	gotList := convertToStringList(got)
	expectList := convertToStringList(expect)
	return reflect.DeepEqual(gotList, expectList)
}

func convertToStringList(in []interface{}) []string {
	var out []string
	for _, x := range in {
		if xc, ok := x.(map[string]string); ok {
			for k, v := range xc {
				out = append(out, fmt.Sprint(k, ":", v))
			}
		} else {
			for k, v := range x.(map[string]interface{}) {
				out = append(out, fmt.Sprint(k, ":", v))
			}
		}
	}
	sort.Strings(out)
	return out
}

func benchmarkQueries(b *testing.B, gen testutil.DatabaseFunc) {
	qs, closer := prepare(b, gen)
	defer closer()

	for _, bench := range queries {
		b.Run(bench.message, func(b *testing.B) {
			if testing.Short() && bench.long {
				b.Skip()
			}
			b.StopTimer()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				func() {
					ctx := context.Background()
					if timeout > 0 {
						var cancel func()
						ctx, cancel = context.WithTimeout(ctx, timeout)
						defer cancel()
					}
					ses := gizmo.NewSession(qs)
					b.StartTimer()
					it, err := ses.Execute(ctx, bench.query, query.Options{
						Collation: query.Raw,
					})
					if err != nil {
						b.Fatal(err)
					}
					defer it.Close()
					n := 0
					for it.Next(ctx) {
						n++
					}
					if err = it.Err(); err != nil {
						b.Fatal(err)
					}
					b.StopTimer()
					if n != len(bench.expect) {
						b.Fatalf("unexpected number of results: %d vs %d", n, len(bench.expect))
					}
				}()
			}
		})
	}
}
