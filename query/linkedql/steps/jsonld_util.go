// Vendored from https://github.com/piprate/json-gold/blob/master/ld/rdf_compare_test.go

package steps

import (
	"log"
	"sort"
	"strconv"
	"strings"

	. "github.com/piprate/json-gold/ld"
)

// Perm calls f with each permutation of a.
func Perm(a []string, f func([]string) bool) {
	perm(a, f, 0)
}

// Permute the values at index i to len(a)-1.
func perm(a []string, f func([]string) bool, i int) bool {
	if i > len(a) {
		return f(a)
	}
	if perm(a, f, i+1) {
		// stop
		return true
	}
	for j := i + 1; j < len(a); j++ {
		a[i], a[j] = a[j], a[i]
		if perm(a, f, i+1) {
			// stop
			return true
		}
		a[i], a[j] = a[j], a[i]
	}
	return false
}

func getBlankNodes(quads []*Quad) []string {
	blankNodeSet := make(map[string]interface{})
	for _, quad := range quads {
		if IsBlankNode(quad.Object) {
			blankNodeSet[quad.Object.GetValue()] = nil
			blankNodeSet[quad.Subject.GetValue()] = nil
		}
	}
	return GetKeys(blankNodeSet)
}

func mapBlankNodes(quads []*Quad, actualBlankNodes []string, mappedBlankNodes []string) []*Quad {
	nodeMap := make(map[string]string, len(actualBlankNodes))
	for i := 0; i < len(actualBlankNodes); i++ {
		nodeMap[actualBlankNodes[i]] = mappedBlankNodes[i]
	}
	res := make([]*Quad, 0, len(quads))
	for _, q := range quads {
		obj := q.Object
		if IsBlankNode(q.Object) {
			obj = NewBlankNode(nodeMap[q.Object.GetValue()])
		}
		subj := q.Subject
		if IsBlankNode(q.Subject) {
			subj = NewBlankNode(nodeMap[q.Subject.GetValue()])
		}
		graph := ""
		if q.Graph != nil {
			graph = q.Graph.GetValue()
		}
		res = append(res, NewQuad(subj, q.Predicate, obj, graph))
	}
	return res
}

func sortNQuads(input string) string {
	temp := strings.Split(input, "\n")
	temp = temp[:len(temp)-1]
	sort.Strings(temp)
	temp = append(temp, "")
	return strings.Join(temp, "\n")
}

// Isomorphic returns true if two given sets of n-quads are isomorphic.
// This is a lazy implementation and it should only be used for testing.
// We build all possible permutations of blank node IDs and try them one
// by one. Minimal optimisations are applied.
func Isomorphic(expectedStr, actualStr string) bool {
	expected := sortNQuads(expectedStr)
	actual := sortNQuads(actualStr)

	// if quads are identical, exit early
	if DeepCompare(expected, actual, true) {
		return true
	}

	serializer := &NQuadRDFSerializer{}

	expectedStr, _ = strconv.Unquote(expectedStr)
	actualStr, _ = strconv.Unquote(actualStr)
	expectedDS, _ := serializer.Parse(expectedStr)
	actualDS, _ := serializer.Parse(actualStr)

	if len(expectedDS.Graphs) != len(actualDS.Graphs) {
		log.Println("Number of graphs doesn't match")
		return false
	}

	for graphName, quads := range expectedDS.Graphs {
		actualQuads := actualDS.Graphs[graphName]
		if len(quads) != len(actualQuads) {
			log.Printf("Number of quads doesn't match in graph %s\n", graphName)
			return false
		}
		expectedBlankNodes := getBlankNodes(quads)
		actualBlankNodes := getBlankNodes(actualQuads)
		if len(expectedBlankNodes) != len(actualBlankNodes) {
			log.Printf("Number of blank nodes doesn't match in graph %s\n", graphName)
			return false
		}

		expectedGraphDS := &RDFDataset{
			Graphs: map[string][]*Quad{
				graphName: quads,
			},
		}
		expectedObj, _ := serializer.Serialize(expectedGraphDS)
		expectedGraph := sortNQuads(expectedObj.(string))

		isomorphic := false
		Perm(expectedBlankNodes, func(perm []string) bool {
			permutedDS := &RDFDataset{
				Graphs: map[string][]*Quad{
					graphName: mapBlankNodes(actualQuads, actualBlankNodes, perm),
				},
			}
			permutedObj, _ := serializer.Serialize(permutedDS)
			permutedGraph := sortNQuads(permutedObj.(string))

			if DeepCompare(expectedGraph, permutedGraph, true) {
				isomorphic = true
				return true
			}
			return false
		})
		if isomorphic {
			return true
		}
	}

	return false
}
