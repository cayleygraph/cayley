package quad

import (
	"github.com/cayleygraph/quad"
)

// Format is a description for quad-file formats.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
type Format = quad.Format

// RegisterFormat registers a new quad-file format.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
func RegisterFormat(f Format) {
	quad.RegisterFormat(f)
}

// FormatByName returns a registered format by its name.
// Will return nil if format is not found.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
func FormatByName(name string) *Format {
	return quad.FormatByName(name)
}

// FormatByExt returns a registered format by its file extension.
// Will return nil if format is not found.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
func FormatByExt(name string) *Format {
	return quad.FormatByExt(name)
}

// FormatByMime returns a registered format by its MIME type.
// Will return nil if format is not found.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
func FormatByMime(name string) *Format {
	return quad.FormatByMime(name)
}

// Formats returns a list of all supported quad formats.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
func Formats() []Format {
	return quad.Formats()
}
