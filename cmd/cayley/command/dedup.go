package command

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"hash"
	"sort"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/path"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc/rdf"
)

func iriFlag(s string, err error) (quad.IRI, error) {
	if err != nil {
		return "", err
	}
	return quad.IRI(s), nil
}

func NewDedupCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dedup",
		Short: "Deduplicate bnode values",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			printBackendInfo()
			h, err := openDatabase()
			if err != nil {
				return err
			}
			defer h.Close()

			pred, _ := iriFlag(cmd.Flags().GetString("pred"))
			typ, _ := iriFlag(cmd.Flags().GetString("type"))
			if typ == "" {
				return errors.New("no type is specified")
			}
			return dedupProperties(ctx, h, pred, typ)
		},
	}
	cmd.Flags().String("pred", rdf.Type, "type predicate to use to find nodes")
	cmd.Flags().String("type", "", "type value to use to find nodes")
	return cmd
}

func valueLess(a, b graph.Ref) bool {
	// TODO(dennwc): more effective way
	s1, s2 := fmt.Sprint(a), fmt.Sprint(b)
	return s1 < s2
}

type sortVals []graph.Ref

func (a sortVals) Len() int           { return len(a) }
func (a sortVals) Less(i, j int) bool { return valueLess(a[i], a[j]) }
func (a sortVals) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

type sortProp []property

func (a sortProp) Len() int           { return len(a) }
func (a sortProp) Less(i, j int) bool { return valueLess(a[i].Pred, a[j].Pred) }
func (a sortProp) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func hashProperties(h hash.Hash, m map[interface{}]property) string {
	props := make([]property, 0, len(m))
	for _, p := range m {
		if len(p.Values) > 1 {
			sort.Sort(sortVals(p.Values))
		}
		props = append(props, p)
	}
	sort.Sort(sortProp(props))
	h.Reset()
	for _, p := range props {
		fmt.Fprint(h, p.Pred)
		h.Write([]byte{0})
		for _, v := range p.Values {
			fmt.Fprint(h, v)
			h.Write([]byte{1})
		}
	}
	res := make([]byte, 0, h.Size())
	res = h.Sum(res)
	return string(res)
}

type property struct {
	Pred   graph.Ref
	Values []graph.Ref
}

func dedupProperties(ctx context.Context, h *graph.Handle, pred, typ quad.IRI) error {
	batch := viper.GetInt(KeyLoadBatch)
	if batch == 0 {
		batch = quad.DefaultBatch
	}

	qs := h.QuadStore
	p := path.StartPath(qs).Has(pred, typ)
	ictx, cancel := context.WithCancel(ctx)
	defer cancel()
	var gerr error

	seen := make(map[string]graph.Ref)
	cnt, dedup := 0, 0
	start := time.Now()
	last := start
	hh := sha1.New()

	tx := graph.NewTransaction()
	txn := 0
	flush := func() {
		if txn == 0 {
			return
		}
		err := h.ApplyTransaction(tx)
		if err == nil {
			tx = graph.NewTransaction()
			dedup += txn
			txn = 0
		} else {
			gerr = err
			cancel()
		}
		if now := time.Now(); now.Sub(last) > time.Second*5 {
			last = now
			clog.Infof("deduplicated %d/%d nodes (%.1f nodes/sec)",
				dedup, cnt, float64(cnt)/now.Sub(start).Seconds(),
			)
		}
	}
	err := p.Iterate(ictx).Each(func(s graph.Ref) {
		cnt++
		it := qs.QuadIterator(quad.Subject, s)
		defer it.Close()
		m := make(map[interface{}]property)
		for it.Next(ictx) {
			q := it.Result()
			p := qs.QuadDirection(q, quad.Predicate)
			o := qs.QuadDirection(q, quad.Object)
			k := graph.ToKey(p)
			prop := m[k]
			prop.Pred = p
			prop.Values = append(prop.Values, o)
			m[k] = prop
		}
		if gerr = it.Err(); gerr != nil {
			cancel()
		}
		ph := hashProperties(hh, m)
		id, ok := seen[ph]
		if !ok {
			seen[ph] = s
			return
		}
		if gerr = dedupValueTx(ictx, h, tx, s, id); gerr != nil {
			cancel()
		}
		txn++
		if txn >= batch { // TODO(dennwc): flag
			flush()
		}
	})
	flush()
	clog.Infof("deduplicated %d/%d nodes in %v", dedup, cnt, time.Since(start))
	if gerr != nil {
		err = gerr
	}
	return err
}

func dedupValueTx(ctx context.Context, h *graph.Handle, tx *graph.Transaction, a, b graph.Ref) error {
	v := h.NameOf(b)
	it := h.QuadIterator(quad.Object, a)
	defer it.Close()
	for it.Next(ctx) {
		// TODO(dennwc): we should be able to add "raw" quads without getting values for directions
		q := h.Quad(it.Result())
		tx.RemoveQuad(q)
		q.Object = v
		tx.AddQuad(q)
	}
	if err := it.Err(); err != nil {
		return err
	}
	it.Close()

	it = h.QuadIterator(quad.Subject, a)
	defer it.Close()
	for it.Next(ctx) {
		q := h.Quad(it.Result())
		tx.RemoveQuad(q)
	}
	if err := it.Err(); err != nil {
		return err
	}
	return nil
}
