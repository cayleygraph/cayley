// Package schema contains constants of the Schema.org vocabulary.
package schema

import "github.com/cayleygraph/cayley/voc"

func init() {
	voc.RegisterPrefix(Prefix, NS)
}

const (
	NS     = `http://schema.org/`
	Prefix = `schema:`
)

const (
	// Types

	// The basic data types such as Integers, Strings, etc.
	DataType = Prefix + `DataType`
	// Boolean: True or False.
	Boolean = Prefix + `Boolean`
	// The boolean value false.
	False = Prefix + `False`
	// The boolean value true.
	True = Prefix + `True`
	// Data type: Text.
	Text = Prefix + `Text`
	// Data type: URL.
	URL = Prefix + `URL`
	// Data type: Number.
	Number = Prefix + `Number`
	// Data type: Floating number.
	Float = Prefix + `Float`
	// Data type: Integer.
	Integer = Prefix + `Integer`
	// A date value in ISO 8601 date format.
	Date = Prefix + `Date`
	// A point in time recurring on multiple days in the form hh:mm:ss[Z|(+|-)hh:mm].
	Time = Prefix + `Time`
	// A combination of date and time of day in the form [-]CCYY-MM-DDThh:mm:ss[Z|(+|-)hh:mm] (see Chapter 5.4 of ISO 8601).
	DateTime = Prefix + `DateTime`

	// A class, also often called a 'Type'; equivalent to rdfs:Class.
	Class = Prefix + "Class"
	// A property, used to indicate attributes and relationships of some Thing; equivalent to rdf:Property.
	Property = Prefix + "Property"
)

const (
	// The name of the item.
	Name    = Prefix + `name`
	UrlProp = Prefix + `url`
)
