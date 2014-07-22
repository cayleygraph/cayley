package db

import (
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"testing"
)

var testDecompressor = []struct {
	message string
	input   []byte
	expect  []byte
	err     error
	readErr error
}{
	{
		message: "text input",
		input:   []byte("cayley data\n"),
		err:     nil,
		expect:  []byte("cayley data\n"),
		readErr: nil,
	},
	{
		message: "gzip input",
		input: []byte{
			0x1f, 0x8b, 0x08, 0x00, 0x5c, 0xbc, 0xcd, 0x53, 0x00, 0x03, 0x4b, 0x4e, 0xac, 0xcc, 0x49, 0xad,
			0x54, 0x48, 0x49, 0x2c, 0x49, 0xe4, 0x02, 0x00, 0x03, 0xe1, 0xfc, 0xc3, 0x0c, 0x00, 0x00, 0x00,
		},
		err:     nil,
		expect:  []byte("cayley data\n"),
		readErr: nil,
	},
	{
		message: "bzip2 input",
		input: []byte{
			0x42, 0x5a, 0x68, 0x39, 0x31, 0x41, 0x59, 0x26, 0x53, 0x59, 0xb5, 0x4b, 0xe3, 0xc4, 0x00, 0x00,
			0x02, 0xd1, 0x80, 0x00, 0x10, 0x40, 0x00, 0x2e, 0x04, 0x04, 0x20, 0x20, 0x00, 0x31, 0x06, 0x4c,
			0x41, 0x4c, 0x1e, 0xa7, 0xa9, 0x2a, 0x18, 0x26, 0xb1, 0xc2, 0xee, 0x48, 0xa7, 0x0a, 0x12, 0x16,
			0xa9, 0x7c, 0x78, 0x80,
		},
		err:     nil,
		expect:  []byte("cayley data\n"),
		readErr: nil,
	},
	{
		message: "bad gzip input",
		input:   []byte{0x1f, 0x8b, 'c', 'a', 'y', 'l', 'e', 'y', ' ', 'd', 'a', 't', 'a', '\n'},
		err:     gzip.ErrHeader,
		expect:  nil,
		readErr: nil,
	},
	{
		message: "bad bzip2 input",
		input:   []byte{0x42, 0x5a, 0x68, 'c', 'a', 'y', 'l', 'e', 'y', ' ', 'd', 'a', 't', 'a', '\n'},
		err:     nil,
		expect:  nil,
		readErr: bzip2.StructuralError("invalid compression level"),
	},
}

func TestDecompressor(t *testing.T) {
	for _, test := range testDecompressor {
		buf := bytes.NewReader(test.input)
		r, err := decompressor(buf)
		if err != test.err {
			t.Fatalf("Unexpected error for %s, got:%v expect:%v", test.message, err, test.err)
		}
		if err != nil {
			continue
		}
		p := make([]byte, len(test.expect)*2)
		n, err := r.Read(p)
		if err != test.readErr {
			t.Fatalf("Unexpected error for reading %s, got:%v expect:%v", test.message, err, test.err)
		}
		if bytes.Compare(p[:n], test.expect) != 0 {
			t.Errorf("Unexpected read result for %s, got:%q expect:%q", test.message, p[:n], test.expect)
		}
	}
}
