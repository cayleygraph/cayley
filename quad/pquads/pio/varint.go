// Extensions for Protocol Buffers to create more go like structures.
//
// Copyright (c) 2013, Vastech SA (PTY) LTD. All rights reserved.
// http://github.com/gogo/protobuf/gogoproto
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package pio

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"

	"github.com/gogo/protobuf/proto"
)

var (
	errSmallBuffer = errors.New("Buffer Too Small")
	errLargeValue  = errors.New("Value is Larger than 64 bits")
)

func NewWriter(w io.Writer) Writer {
	return &varintWriter{w: w, lenBuf: make([]byte, binary.MaxVarintLen64)}
}

type varintWriter struct {
	w      io.Writer
	lenBuf []byte
	buffer []byte
}

func (w *varintWriter) WriteMsg(msg proto.Message) (_ int, err error) {
	var data []byte
	if m, ok := msg.(marshaler); ok {
		n, ok := getSize(m)
		if !ok {
			data, err = proto.Marshal(msg)
			if err != nil {
				return 0, err
			}
		}
		if n >= len(w.buffer) {
			w.buffer = make([]byte, n)
		}
		_, err = m.MarshalTo(w.buffer)
		if err != nil {
			return 0, err
		}
		data = w.buffer[:n]
	} else {
		data, err = proto.Marshal(msg)
		if err != nil {
			return 0, err
		}
	}
	length := uint64(len(data))
	n := binary.PutUvarint(w.lenBuf, length)
	n, err = w.w.Write(w.lenBuf[:n])
	if err != nil {
		return n, err
	}
	nd, err := w.w.Write(data)
	return n + nd, err
}

func NewReader(r io.Reader, maxSize int) Reader {
	return &varintReader{r: bufio.NewReader(r), maxSize: maxSize}
}

type varintReader struct {
	r       *bufio.Reader
	buf     []byte
	maxSize int

	readLen bool
	len     int
}

func (r *varintReader) readLength() error {
	if r.readLen {
		return nil
	}
	length64, err := binary.ReadUvarint(r.r)
	if err != nil {
		return err
	}
	length := int(length64)
	r.readLen, r.len = true, length
	return nil
}

func (r *varintReader) SkipMsg() error {
	if err := r.readLength(); err != nil {
		return err
	}
	if r.len < 0 {
		return io.ErrShortBuffer
	}
	r.readLen = false
	if _, err := r.r.Discard(r.len); err != nil {
		return err
	}
	return nil
}

func (r *varintReader) ReadMsg(msg proto.Message) error {
	if err := r.readLength(); err != nil {
		return err
	}
	if r.len < 0 || r.len > r.maxSize {
		return io.ErrShortBuffer
	}
	r.readLen = false
	if len(r.buf) < r.len {
		r.buf = make([]byte, r.len)
	}
	buf := r.buf[:r.len]
	if _, err := io.ReadFull(r.r, buf); err != nil {
		return err
	}
	return proto.Unmarshal(buf, msg)
}
