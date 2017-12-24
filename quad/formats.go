package quad

import (
	"fmt"
	"io"
)

// Format is a description for quad-file formats.
type Format struct {
	// Name is a short format name used as identifier for RegisterFormat.
	Name string
	// Ext is a list of file extensions, allowed for file format. Can be used to detect file format, given a path.
	Ext []string
	// Mime is a list of MIME (content) types, allowed for file format. Can be used in HTTP request/responses.
	Mime []string
	// Reader is a function for creating format reader, that reads serialized data from io.Reader.
	Reader func(io.Reader) ReadCloser
	// Writer is a function for creating format writer, that streams serialized data to io.Writer.
	Writer func(io.Writer) WriteCloser
	// Binary is set to true if format is not human-readable.
	Binary bool
	// MarshalValue encodes one value in specific a format.
	MarshalValue func(v Value) ([]byte, error)
	// UnmarshalValue decodes a value from specific format.
	UnmarshalValue func(b []byte) (Value, error)
}

var (
	formatsByName = make(map[string]*Format)
	formatsByExt  = make(map[string]*Format)
	formatsByMime = make(map[string]*Format)
)

// RegisterFormat registers a new quad-file format.
func RegisterFormat(f Format) {
	if _, ok := formatsByName[f.Name]; ok {
		panic(fmt.Errorf("format %s is allready registered", f.Name))
	}
	formatsByName[f.Name] = &f
	for _, m := range f.Ext {
		if sf, ok := formatsByExt[m]; ok {
			panic(fmt.Errorf("format %s is allready registered with MIME %s", sf.Name, m))
		}
		formatsByExt[m] = &f
	}
	for _, m := range f.Mime {
		if sf, ok := formatsByMime[m]; ok {
			panic(fmt.Errorf("format %s is allready registered with MIME %s", sf.Name, m))
		}
		formatsByMime[m] = &f
	}
}

// FormatByName returns a registered format by its name.
// Will return nil if format is not found.
func FormatByName(name string) *Format {
	return formatsByName[name]
}

// FormatByExt returns a registered format by its file extension.
// Will return nil if format is not found.
func FormatByExt(name string) *Format {
	return formatsByExt[name]
}

// FormatByMime returns a registered format by its MIME type.
// Will return nil if format is not found.
func FormatByMime(name string) *Format {
	return formatsByMime[name]
}

// Formats returns a list of all supported quad formats.
func Formats() []Format {
	list := make([]Format, 0, len(formatsByName))
	for _, f := range formatsByName {
		list = append(list, *f)
	}
	return list
}
