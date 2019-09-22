// Package schema is deprecated. Use github.com/cayleygraph/quad/voc/schema.
package schema

import "github.com/cayleygraph/quad/voc/schema"

const (
	NS     = schema.NS
	Prefix = schema.Prefix
)

const (
	// Types

	// The basic data types such as Integers, Strings, etc.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/schema package instead.
	DataType = schema.DataType
	// Boolean: True or False.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/schema package instead.
	Boolean = schema.Boolean
	// The boolean value false.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/schema package instead.
	False = schema.False
	// The boolean value true.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/schema package instead.
	True = schema.True
	// Data type: Text.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/schema package instead.
	Text = schema.Text
	// Data type: URL.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/schema package instead.
	URL = schema.URL
	// Data type: Number.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/schema package instead.
	Number = schema.Number
	// Data type: Floating number.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/schema package instead.
	Float = schema.Float
	// Data type: Integer.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/schema package instead.
	Integer = schema.Integer
	// A date value in ISO 8601 date format.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/schema package instead.
	Date = schema.Date
	// A point in time recurring on multiple days in the form hh:mm:ss[Z|(+|-)hh:mm].
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/schema package instead.
	Time = schema.Time
	// A combination of date and time of day in the form [-]CCYY-MM-DDThh:mm:ss[Z|(+|-)hh:mm] (see Chapter 5.4 of ISO 8601).
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/schema package instead.
	DateTime = schema.DateTime

	// A class, also often called a 'Type'; equivalent to rdfs:Class.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/schema package instead.
	Class = schema.Class
	// A property, used to indicate attributes and relationships of some Thing; equivalent to rdf:Property.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/schema package instead.
	Property = schema.Property
)

const (
	// The name of the item.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/schema package instead.
	Name    = schema.Name
	UrlProp = schema.UrlProp
)
