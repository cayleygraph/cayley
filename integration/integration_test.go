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

package integration

import (
	"flag"
	"fmt"
	"io"
	"os"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/internal"
	"github.com/cayleygraph/cayley/internal/config"
	"github.com/cayleygraph/cayley/internal/db"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/gremlin"

	// Load all supported backends.
	_ "github.com/cayleygraph/cayley/graph/bolt"
	_ "github.com/cayleygraph/cayley/graph/leveldb"
	_ "github.com/cayleygraph/cayley/graph/memstore"
	_ "github.com/cayleygraph/cayley/graph/mongo"
	_ "github.com/cayleygraph/cayley/graph/sql"

	// Load writer registry
	_ "github.com/cayleygraph/cayley/writer"
)

const format = "cquad"

var backend = flag.String("backend", "memstore", "Which backend to test. Loads test data to /tmp if not present.")
var backendPath = flag.String("backend_path", "", "Path to the chosen backend. Will have sane testing defaults if not specified")

var benchmarkQueries = []struct {
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
		g.V("Humphrey Bogart").In("<name>").All()
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
		function getId(x) { return g.V(x).In("<name>") }
		var actor_to_film = g.M().In("</film/performance/actor>").In("</film/film/starring>")

		getId("Oliver Hardy").Follow(actor_to_film).Out("<name>").Intersect(
			getId("Mel Blanc").Follow(actor_to_film).Out("<name>")).All()
			`,
		expect: nil,
	},

	// 2014-07-12: This one takes about 4 whole seconds in memory. This is a behemoth.
	{
		message: "three huge sets with small intersection",
		long:    true,
		query: `
			function getId(x) { return g.V(x).In("<name>") }
			var actor_to_film = g.M().In("</film/performance/actor>").In("</film/film/starring>")

			var a = getId("Oliver Hardy").Follow(actor_to_film).FollowR(actor_to_film)
			var b = getId("Mel Blanc").Follow(actor_to_film).FollowR(actor_to_film)
			var c = getId("Billy Gilbert").Follow(actor_to_film).FollowR(actor_to_film)

			seen = {}

			a.Intersect(b).Intersect(c).ForEach(function (d) {
				if (!(d.id in seen)) {
					seen[d.id] = true;
					g.Emit(d)
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
			g.V().As("person").In("<name>").In().In().Out("<name>").Is("Casablanca").All()
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
			g.V().As("person").In("<name>").In().In().Out("<name>").Except(g.V("Ingrid Bergman").In("<name>").In().In().Out("<name>")).Is("Casablanca").All()
			`,
		tag:    "person",
		expect: nil,
	},
	{
		message: "the helpless checker, negated (without actors Ingrid Bergman)",
		long:    true,
		query: `
			g.V().As("person").In("<name>").Except(g.V("Ingrid Bergman").In("<name>")).In().In().Out("<name>").Is("Casablanca").All()
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
		query: common + `m1_actors.Intersect(m2_actors).Out("<name>").All()
`,
		expect: []interface{}{
			map[string]string{"id": "Sandra Bullock", "movie1": "The Net", "movie2": "Speed"},
		},
	},

	//Q: Did "Keanu Reeves" star in "The Net" ?
	//A: No
	{
		message: "Keanu in The Net",
		query: common + `actor2.Intersect(m1_actors).Out("<name>").All()
`,
		expect: nil,
	},

	//Q: Did "Keanu Reeves" star in "Speed" ?
	//A: Yes
	{
		message: "Keanu in Speed",
		query: common + `actor2.Intersect(m2_actors).Out("<name>").All()
`,
		expect: []interface{}{
			map[string]string{"id": "Keanu Reeves", "movie2": "Speed"},
		},
	},

	//Q: Has "Keanu Reeves" co-starred with anyone who starred in "The Net" ?
	//A: "Keanu Reeves" was in "Speed" and "The Lake House" with "Sandra Bullock",
	//   who was in "The Net"
	{
		message: "Keanu with other in The Net",
		long:    true,
		query: common + `actor2.Follow(coStars1).Intersect(m1_actors).Out("<name>").All()
`,
		expect: []interface{}{
			map[string]string{"id": "Sandra Bullock", "movie1": "The Net", "costar1_movie": "Speed"},
			map[string]string{"movie1": "The Net", "costar1_movie": "The Lake House", "id": "Sandra Bullock"},
		},
	},

	//Q: Do "Keanu Reeves" and "Sandra Bullock" have any commons co-stars?
	//A: Yes, many. For example: SB starred with "Steve Martin" in "The Prince
	//    of Egypt", and KR starred with Steven Martin in "Parenthood".
	{
		message: "Keanu and Bullock with other",
		long:    true,
		query: common + `actor1.Save("<name>","costar1_actor").Follow(coStars1).Intersect(actor2.Save("<name>","costar2_actor").Follow(coStars2)).Out("<name>").All()
`,
		expect: []interface{}{
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Proposal", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Proposal", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Proposal", "costar2_actor": "Keanu Reeves", "costar2_movie": "Parenthood", "id": "Mary Steenburgen"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Proposal", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Devil's Advocate", "id": "Craig T. Nelson"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Crash", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Crash", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Gun Shy", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Gun Shy", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Demolition Man", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Demolition Man", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Demolition Man", "costar2_actor": "Keanu Reeves", "costar2_movie": "Thumbsucker", "id": "Benjamin Bratt"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Divine Secrets of the Ya-Ya Sisterhood", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Divine Secrets of the Ya-Ya Sisterhood", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Divine Secrets of the Ya-Ya Sisterhood", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Private Lives of Pippa Lee", "id": "Shirley Knight"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "A Time to Kill", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "A Time to Kill", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Forces of Nature", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Forces of Nature", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Hope Floats", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Hope Floats", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Infamous", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Infamous", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Infamous", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Jeff Daniels"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Love Potion No. 9", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Love Potion No. 9", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Miss Congeniality", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Miss Congeniality", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Miss Congeniality", "costar2_actor": "Keanu Reeves", "costar2_movie": "Thumbsucker", "id": "Benjamin Bratt"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Miss Congeniality 2: Armed and Fabulous", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Miss Congeniality 2: Armed and Fabulous", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Murder by Numbers", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Murder by Numbers", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Practical Magic", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Practical Magic", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Practical Magic", "costar2_actor": "Keanu Reeves", "costar2_movie": "Parenthood", "id": "Dianne Wiest"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Flying", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Animatrix", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Tune in Tomorrow", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Last Time I Committed Suicide", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Constantine", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Permanent Record", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Dangerous Liaisons", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Private Lives of Pippa Lee", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "A Scanner Darkly", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "A Walk in the Clouds", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Hardball", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Life Under Water", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Much Ado About Nothing", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "My Own Private Idaho", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Parenthood", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Point Break", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Providence", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "River's Edge", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Something's Gotta Give", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Sweet November", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Matrix Reloaded", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Matrix Revisited", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Prince of Pennsylvania", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Replacements", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Even Cowgirls Get the Blues", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Youngblood", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Bill & Ted's Bogus Journey", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Bill & Ted's Excellent Adventure", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Johnny Mnemonic", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Devil's Advocate", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Thumbsucker", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "I Love You to Death", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Bram Stoker's Dracula", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Gift", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Little Buddha", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Night Watchman", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Chain Reaction", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Babes in Toyland", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Day the Earth Stood Still", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "River's Edge", "id": "Dennis Hopper"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Dennis Hopper"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Jeff Daniels"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Joe Morton"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Alan Ruck"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Glenn Plummer"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Carlos Carrasco"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Beth Grant"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Richard Lineback"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Hawthorne James"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Jordan Lund"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Thomas Rosales, Jr."},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed 2: Cruise Control", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed 2: Cruise Control", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Speed 2: Cruise Control", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Glenn Plummer"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "Flying", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Animatrix", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "Tune in Tomorrow", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Last Time I Committed Suicide", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "Constantine", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "Permanent Record", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "Dangerous Liaisons", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Private Lives of Pippa Lee", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "A Scanner Darkly", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "A Walk in the Clouds", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "Hardball", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "Life Under Water", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "Much Ado About Nothing", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "My Own Private Idaho", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "Parenthood", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "Point Break", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "Providence", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "River's Edge", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "Something's Gotta Give", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "Sweet November", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Matrix Reloaded", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Matrix Revisited", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Prince of Pennsylvania", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Replacements", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "Even Cowgirls Get the Blues", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "Youngblood", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "Bill & Ted's Bogus Journey", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "Bill & Ted's Excellent Adventure", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "Johnny Mnemonic", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Devil's Advocate", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "Thumbsucker", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "I Love You to Death", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "Bram Stoker's Dracula", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Gift", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "Little Buddha", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Night Watchman", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "Chain Reaction", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "Babes in Toyland", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Day the Earth Stood Still", "id": "Keanu Reeves"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Christopher Plummer"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Dylan Walsh"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Shohreh Aghdashloo"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Lake House", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Lynn Collins"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Net", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Net", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Prince of Egypt", "costar2_actor": "Keanu Reeves", "costar2_movie": "Dangerous Liaisons", "id": "Michelle Pfeiffer"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Prince of Egypt", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Prince of Egypt", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Prince of Egypt", "costar2_actor": "Keanu Reeves", "costar2_movie": "Parenthood", "id": "Steve Martin"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Two Weeks Notice", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Two Weeks Notice", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "While You Were Sleeping", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "While You Were Sleeping", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "While You Were Sleeping", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Replacements", "id": "Jack Warden"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "28 Days", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "28 Days", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Premonition", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Premonition", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Premonition", "costar2_actor": "Keanu Reeves", "costar2_movie": "Constantine", "id": "Peter Stormare"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Wrestling Ernest Hemingway", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Wrestling Ernest Hemingway", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Fire on the Amazon", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "Fire on the Amazon", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Thing Called Love", "costar2_actor": "Keanu Reeves", "costar2_movie": "My Own Private Idaho", "id": "River Phoenix"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Thing Called Love", "costar2_actor": "Keanu Reeves", "costar2_movie": "I Love You to Death", "id": "River Phoenix"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Thing Called Love", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "The Thing Called Love", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "In Love and War", "costar2_actor": "Keanu Reeves", "costar2_movie": "Speed", "id": "Sandra Bullock"},
			map[string]string{"costar1_actor": "Sandra Bullock", "costar1_movie": "In Love and War", "costar2_actor": "Keanu Reeves", "costar2_movie": "The Lake House", "id": "Sandra Bullock"},
		},
	},
	{
		message: "Save a number of predicates around a set of nodes",
		query: `
		g.V("_:9037", "_:49278", "_:44112", "_:44709", "_:43382").Save("</film/performance/character>", "char").Save("</film/performance/actor>", "act").SaveR("</film/film/starring>", "film").All()
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
var movie1 = g.V().Has("<name>", "The Net")
var movie2 = g.V().Has("<name>", "Speed")
var actor1 = g.V().Has("<name>", "Sandra Bullock")
var actor2 = g.V().Has("<name>", "Keanu Reeves")

// (film) -> starring -> (actor)
var filmToActor = g.Morphism().Out("</film/film/starring>").Out("</film/performance/actor>")

// (actor) -> starring -> [film -> starring -> (actor)]
var coStars1 = g.Morphism().In("</film/performance/actor>").In("</film/film/starring>").Save("<name>","costar1_movie").Follow(filmToActor)
var coStars2 = g.Morphism().In("</film/performance/actor>").In("</film/film/starring>").Save("<name>","costar2_movie").Follow(filmToActor)

// Stars for the movies "The Net" and "Speed"
var m1_actors = movie1.Save("<name>","movie1").Follow(filmToActor)
var m2_actors = movie2.Save("<name>","movie2").Follow(filmToActor)
`

var (
	create            sync.Once
	deleteAndRecreate sync.Once
	cfg               = &config.Config{
		ReplicationType: "single",
		Timeout:         300 * time.Second,
	}

	handle *graph.Handle
)

func prepare(t testing.TB) {
	var remote bool
	cfg.DatabaseType = *backend
	switch *backend {
	case "memstore":
		cfg.DatabasePath = "../data/30kmoviedata.nq.gz"
	case "leveldb", "bolt":
		cfg.DatabasePath = "/tmp/cayley_test_" + *backend
		cfg.DatabaseOptions = map[string]interface{}{
			"nosync": true, // It's a test. If we need to load, do it fast.
		}
	case "mongo":
		cfg.DatabasePath = "localhost:27017"
		cfg.DatabaseOptions = map[string]interface{}{
			"database_name": "cayley_test", // provide a default test database
		}
		remote = true
	case "sql":
		cfg.DatabasePath = "postgres://localhost/cayley_test"
		remote = true
	default:
		t.Fatalf("Untestable backend store %s", *backend)
	}
	if *backendPath != "" {
		cfg.DatabasePath = *backendPath
	}

	var err error
	create.Do(func() {
		needsLoad := true
		if graph.IsPersistent(cfg.DatabaseType) && !remote {
			if _, err := os.Stat(cfg.DatabasePath); os.IsNotExist(err) {
				err = db.Init(cfg)
				if err != nil {
					t.Fatalf("Could not initialize database: %v", err)
				}
			} else {
				needsLoad = false
			}
		}

		handle, err = db.Open(cfg)
		if err != nil {
			t.Fatalf("Failed to open %q: %v", cfg.DatabasePath, err)
		}

		if needsLoad && !remote {
			err = internal.Load(handle.QuadWriter, cfg, "../data/30kmoviedata.nq.gz", format)
			if err != nil {
				t.Fatalf("Failed to load %q: %v", cfg.DatabasePath, err)
			}
		}
	})
}

func deletePrepare(t testing.TB) {
	var err error
	deleteAndRecreate.Do(func() {
		prepare(t)
		if !graph.IsPersistent(cfg.DatabaseType) {
			err = removeAll(handle.QuadWriter, cfg, "", format)
			if err != nil {
				t.Fatalf("Failed to remove %q: %v", cfg.DatabasePath, err)
			}
			err = internal.Load(handle.QuadWriter, cfg, "", format)
			if err != nil {
				t.Fatalf("Failed to load %q: %v", cfg.DatabasePath, err)
			}
		}
	})
}

func removeAll(qw graph.QuadWriter, cfg *config.Config, path, typ string) error {
	return internal.DecompressAndLoad(qw, cfg, path, typ, remove)
}

func remove(qw graph.QuadWriter, cfg *config.Config, dec quad.Reader) error {
	for {
		t, err := dec.ReadQuad()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		err = qw.RemoveQuad(t)
		if err != nil {
			return err
		}
	}
	return nil
}

func TestQueries(t *testing.T) {
	prepare(t)
	checkQueries(t)
}

func TestDeletedAndRecreatedQueries(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	deletePrepare(t)
	checkQueries(t)
}

func checkQueries(t *testing.T) {
	for _, test := range benchmarkQueries {
		if testing.Short() && test.long {
			continue
		}
		if test.skip {
			continue
		}
		func() {
			tInit := time.Now()
			t.Logf("Now testing %s ", test.message)
			ses := gremlin.NewSession(handle.QuadStore, true)
			c := make(chan query.Result, 5)
			ctx := context.Background()
			if cfg.Timeout > 0 {
				var cancel func()
				ctx, cancel = context.WithTimeout(ctx, cfg.Timeout)
				defer cancel()
			}
			go ses.Execute(ctx, test.query, c, 100)
			var got []interface{}
			for r := range c {
				if err := r.Err(); err != nil {
					t.Error("Error:", err)
					continue
				}
				ses.Collate(r)
				j, err := ses.Results()
				if j == nil && err == nil {
					continue
				}
				got = append(got, j.([]interface{})...)
			}
			t.Logf("(%v)\n", time.Since(tInit))

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
		}()
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

func runBench(n int, b *testing.B) {
	if testing.Short() && benchmarkQueries[n].long {
		b.Skip()
	}
	prepare(b)
	b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := make(chan query.Result, 5)
		ctx := context.Background()
		var cancel func()
		if cfg.Timeout > 0 {
			ctx, cancel = context.WithTimeout(ctx, cfg.Timeout)
		}
		ses := gremlin.NewSession(handle.QuadStore, true)
		b.StartTimer()
		go ses.Execute(ctx, benchmarkQueries[n].query, c, 100)
		for range c {
		}
		b.StopTimer()
		if cancel != nil {
			cancel()
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

func BenchmarkHelplessNotContainsFilms(b *testing.B) {
	runBench(4, b)
}

func BenchmarkHelplessNotContainsActors(b *testing.B) {
	runBench(5, b)
}

func BenchmarkNetAndSpeed(b *testing.B) {
	runBench(6, b)
}

func BenchmarkKeanuAndNet(b *testing.B) {
	runBench(7, b)
}

func BenchmarkKeanuAndSpeed(b *testing.B) {
	runBench(8, b)
}

func BenchmarkKeanuOther(b *testing.B) {
	runBench(9, b)
}

func BenchmarkKeanuBullockOther(b *testing.B) {
	runBench(10, b)
}

func BenchmarkSaveBogartPerformances(b *testing.B) {
	runBench(11, b)
}

// reader is a test helper to filter non-io.Reader methods from the contained io.Reader.
type reader struct {
	r io.Reader
}

func (r reader) Read(p []byte) (int, error) {
	return r.r.Read(p)
}
