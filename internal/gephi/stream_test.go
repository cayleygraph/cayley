package gephi

import (
	"bytes"
	"testing"

	"github.com/cayleygraph/quad"
	"github.com/stretchr/testify/require"
)

func TestStreamEncoder(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	gs := NewGraphStream(buf)
	p := map[quad.Value]quad.Value{iriPosX: quad.Float(0), iriPosY: quad.Float(0)}
	gs.AddNode(quad.String("aaa"), p)
	gs.AddNode(quad.String("bbb"), p)
	gs.Flush()
	const expect = "{\"an\":{\"0\":{\"label\":\"aaa\",\"size\":20,\"x\":0,\"y\":0}}}\r\n{\"an\":{\"1\":{\"label\":\"bbb\",\"size\":20,\"x\":0,\"y\":0}}}\r\n"
	require.Equal(t, expect, buf.String())
}
