package bolt

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var casesDecode = []struct {
	Name string
	Data string
	Exp  interface{}
}{
	{
		Name: "tiny int",
		Data: `2A`,
		Exp:  int(42),
	},
	{
		Name: "int8",
		Data: `C8 2A`,
		Exp:  int(42),
	},
	{
		Name: "int16",
		Data: `C9 00 2A`,
		Exp:  int(42),
	},
	{
		Name: "int32",
		Data: `CA 00 00 00 2A`,
		Exp:  int(42),
	},
	{
		Name: "int64",
		Data: `CB 00 00 00 00 00 00 00 2A`,
		Exp:  int(42),
	},
	{
		Name: "min int",
		Data: `CB 80 00 00  00 00 00 00  00`,
		Exp:  int(-9223372036854775808),
	},
	{
		Name: "max int",
		Data: `CB 7F FF FF  FF FF FF FF  FF`,
		Exp:  int(9223372036854775807),
	},
	{
		Name: "tiny string",
		Data: `81 61`,
		Exp:  "a",
	},
	{
		Name: "regular string",
		Data: `D0 1A 61 62  63 64 65 66  67 68 69 6A  6B 6C 6D 6E
6F 70 71 72  73 74 75 76  77 78 79 7A`,
		Exp: "abcdefghijklmnopqrstuvwxyz",
	},
	{
		Name: "utf8 string",
		Data: `D0 18 45 6E  20 C3 A5 20  66 6C C3 B6  74 20 C3 B6
76 65 72 20  C3 A4 6E 67  65 6E`,
		Exp: "En å flöt över ängen",
	},
	{
		Name: "empty list",
		Data: `90`,
		Exp:  []interface{}{},
	},
	{
		Name: "tiny list",
		Data: `93 01 02 03`,
		Exp:  []interface{}{1, 2, 3},
	},
	{
		Name: "regular list",
		Data: `D4 14 01 02  03 04 05 06  07 08 09 00  01 02 03 04
05 06 07 08  09 00`,
		Exp: []interface{}{
			1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
			1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		},
	},
	{
		Name: "empty map",
		Data: `A0`,
		Exp:  map[string]interface{}{},
	},
	{
		Name: "tiny map",
		Data: `A1 81 61 01`,
		Exp:  map[string]interface{}{"a": 1},
	},
	{
		Name: "regular map",
		Data: `D8 10 81 61  01 81 62 01  81 63 03 81  64 04 81 65
05 81 66 06  81 67 07 81  68 08 81 69  09 81 6A 00
81 6B 01 81  6C 02 81 6D  03 81 6E 04  81 6F 05 81
70 06`,
		Exp: map[string]interface{}{
			"a": 1, "b": 1, "c": 3, "d": 4, "e": 5, "f": 6, "g": 7, "h": 8, "i": 9, "j": 0,
			"k": 1, "l": 2, "m": 3, "n": 4, "o": 5, "p": 6,
		},
	},
	{
		Name: "INIT",
		Data: `B1 01 8C 4D  79 43 6C 69  65 6E 74 2F  31 2E 30 A3
86 73 63 68  65 6D 65 85  62 61 73 69  63 89 70 72
69 6E 63 69  70 61 6C 85  6E 65 6F 34  6A 8B 63 72
65 64 65 6E  74 69 61 6C  73 86 73 65  63 72 65 74`,
		Exp: &InitMsg{
			ClientName: "MyClient/1.0",
			AuthToken: map[string]interface{}{
				"scheme": "basic", "principal": "neo4j", "credentials": "secret",
			},
		},
	},
	{
		Name: "RUN",
		Data: `B2 10 8F 52  45 54 55 52  4E 20 31 20  41 53 20 6E  75 6D A0`,
		Exp: &RunMsg{
			Statement:  "RETURN 1 AS num",
			Parameters: map[string]interface{}{},
		},
	},
}

func TestDecode(t *testing.T) {
	rep := strings.NewReplacer(" ", "", "\n", "")
	for _, c := range casesDecode {
		t.Run(c.Name, func(t *testing.T) {
			ds := rep.Replace(c.Data)
			data, err := hex.DecodeString(ds)
			require.NoError(t, err)
			out, n, err := decodeMsg(data)
			require.NoError(t, err)
			require.Equal(t, int(len(data)), int(n))
			require.Equal(t, c.Exp, out)
		})
	}
}
