package quad

import (
	"crypto/sha1"
	"fmt"
	"hash"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cayleygraph/cayley/voc"
	"github.com/cayleygraph/cayley/voc/schema"
)

// Value is a type used by all quad directions.
type Value interface {
	String() string
	// Native converts Value to a closest native Go type.
	//
	// If type has no analogs in Go, Native return an object itself.
	Native() interface{}
}

type TypedStringer interface {
	TypedString() TypedString
}

// Equaler interface is implemented by values, that needs a special equality check.
type Equaler interface {
	Equal(v Value) bool
}

// HashSize is a size of the slice, returned by HashOf.
const HashSize = sha1.Size

var hashPool = sync.Pool{
	New: func() interface{} { return sha1.New() },
}

// HashOf calculates a hash of value v.
func HashOf(v Value) []byte {
	key := make([]byte, HashSize)
	HashTo(v, key)
	return key
}

// HashTo calculates a hash of value v, storing it in a slice p.
func HashTo(v Value, p []byte) {
	h := hashPool.Get().(hash.Hash)
	h.Reset()
	defer hashPool.Put(h)
	if len(p) < HashSize {
		panic("buffer too small to fit the hash")
	}
	if v != nil {
		// TODO(kortschak,dennwc) Remove dependence on String() method.
		h.Write([]byte(v.String()))
	}
	h.Sum(p[:0])
}

// StringOf safely call v.String, returning empty string in case of nil Value.
func StringOf(v Value) string {
	if v == nil {
		return ""
	}
	return v.String()
}

// NativeOf safely call v.Native, returning nil in case of nil Value.
func NativeOf(v Value) interface{} {
	if v == nil {
		return nil
	}
	return v.Native()
}

// AsValue converts native type into closest Value representation.
// It returns false if type was not recognized.
func AsValue(v interface{}) (out Value, ok bool) {
	if v == nil {
		return nil, true
	}
	switch v := v.(type) {
	case Value:
		out = v
	case string:
		out = String(v)
	case int:
		out = Int(v)
	case int64:
		out = Int(v)
	case int32:
		out = Int(v)
	case float64:
		out = Float(v)
	case float32:
		out = Float(v)
	case bool:
		out = Bool(v)
	case time.Time:
		out = Time(v)
	default:
		return nil, false
	}
	return out, true
}

// StringToValue is a function to convert strings to typed
// quad values.
//
// Warning: should not be used directly - will be deprecated.
func StringToValue(v string) Value {
	if v == "" {
		return nil
	}
	if len(v) > 2 {
		if v[0] == '<' && v[len(v)-1] == '>' {
			return IRI(v[1 : len(v)-1])
		} else if v[:2] == "_:" {
			return BNode(v[2:])
		}
	}
	return String(v)
}

// Raw is a Turtle/NQuads-encoded value.
type Raw string

func (s Raw) String() string      { return string(s) }
func (s Raw) Native() interface{} { return s }

// String is an RDF string value (ex: "name").
type String string

var escaper = strings.NewReplacer(
	"\\", "\\\\",
	"\"", "\\\"",
	"\n", "\\n",
	"\r", "\\r",
	"\t", "\\t",
)

func (s String) String() string {
	//TODO(barakmich): Proper escaping.
	return `"` + escaper.Replace(string(s)) + `"`
}
func (s String) Native() interface{} { return string(s) }

// TypedString is an RDF value with type (ex: "name"^^<type>).
type TypedString struct {
	Value String
	Type  IRI
}

func (s TypedString) String() string {
	return s.Value.String() + `^^` + s.Type.String()
}
func (s TypedString) Native() interface{} {
	if s.Type == "" {
		return s.Value.Native()
	}
	if v, err := s.ParseValue(); err == nil && v != s {
		return v.Native()
	}
	return s
}

// ParseValue will try to parse underlying string value using registered functions.
//
// It will return unchanged value if suitable function is not available.
//
// Error will be returned if the type was recognizes, but parsing failed.
func (s TypedString) ParseValue() (Value, error) {
	fnc := knownConversions[s.Type.Full()]
	if fnc == nil {
		return s, nil
	}
	return fnc(string(s.Value))
}

// LangString is an RDF string with language (ex: "name"@lang).
type LangString struct {
	Value String
	Lang  string
}

func (s LangString) String() string {
	return s.Value.String() + `@` + s.Lang
}
func (s LangString) Native() interface{} { return s.Value.Native() }

// IRI is an RDF Internationalized Resource Identifier (ex: <name>).
type IRI string

func (s IRI) String() string      { return `<` + string(s) + `>` }
func (s IRI) Short() IRI          { return IRI(voc.ShortIRI(string(s))) }
func (s IRI) Full() IRI           { return IRI(voc.FullIRI(string(s))) }
func (s IRI) Native() interface{} { return s }
func (s IRI) ShortWith(n *voc.Namespaces) IRI {
	return IRI(n.ShortIRI(string(s)))
}
func (s IRI) FullWith(n *voc.Namespaces) IRI {
	return IRI(n.FullIRI(string(s)))
}

// BNode is an RDF Blank Node (ex: _:name).
type BNode string

func (s BNode) String() string      { return `_:` + string(s) }
func (s BNode) Native() interface{} { return s }

// Native support for basic types

// StringConversion is a function to convert string values with a
// specific IRI type to their native equivalents.
type StringConversion func(string) (Value, error)

const (
	nsXSD = `http://www.w3.org/2001/XMLSchema#`
)

// TODO(dennwc): make these configurable
const (
	defaultIntType   IRI = schema.Integer
	defaultFloatType IRI = schema.Float
	defaultBoolType  IRI = schema.Boolean
	defaultTimeType  IRI = schema.DateTime
)

func init() {
	// int types
	RegisterStringConversion(defaultIntType, stringToInt)
	RegisterStringConversion(nsXSD+`integer`, stringToInt)
	RegisterStringConversion(nsXSD+`long`, stringToInt)
	// bool types
	RegisterStringConversion(defaultBoolType, stringToBool)
	RegisterStringConversion(nsXSD+`boolean`, stringToBool)
	// float types
	RegisterStringConversion(defaultFloatType, stringToFloat)
	RegisterStringConversion(nsXSD+`double`, stringToFloat)
	// time types
	RegisterStringConversion(defaultTimeType, stringToTime)
	RegisterStringConversion(nsXSD+`dateTime`, stringToTime)
}

var knownConversions = make(map[IRI]StringConversion)

// RegisterStringConversion will register an automatic conversion of
// TypedString values with provided type to a native equivalent such as Int, Time, etc.
//
// If fnc is nil, automatic conversion from selected type will be removed.
func RegisterStringConversion(dataType IRI, fnc StringConversion) {
	if fnc == nil {
		delete(knownConversions, dataType)
	} else {
		knownConversions[dataType] = fnc
		if short := dataType.Short(); short != dataType {
			knownConversions[short] = fnc
		}
		if full := dataType.Full(); full != dataType {
			knownConversions[full] = fnc
		}
	}
}

func stringToInt(s string) (Value, error) {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil, err
	}
	return Int(v), nil
}

func stringToBool(s string) (Value, error) {
	v, err := strconv.ParseBool(s)
	if err != nil {
		return nil, err
	}
	return Bool(v), nil
}

func stringToFloat(s string) (Value, error) {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, err
	}
	return Float(v), nil
}

func stringToTime(s string) (Value, error) {
	v, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil, err
	}
	return Time(v), nil
}

// Int is a native wrapper for int64 type.
//
// It uses NQuad notation similar to TypedString.
type Int int64

func (s Int) String() string {
	return s.TypedString().String()
}
func (s Int) Native() interface{} { return int(s) }
func (s Int) TypedString() TypedString {
	return TypedString{
		Value: String(strconv.Itoa(int(s))),
		Type:  defaultIntType,
	}
}

// Float is a native wrapper for float64 type.
//
// It uses NQuad notation similar to TypedString.
type Float float64

func (s Float) String() string {
	return s.TypedString().String()
}
func (s Float) Native() interface{} { return float64(s) }
func (s Float) TypedString() TypedString {
	return TypedString{
		Value: String(strconv.FormatFloat(float64(s), 'E', -1, 64)),
		Type:  defaultFloatType,
	}
}

// Bool is a native wrapper for bool type.
//
// It uses NQuad notation similar to TypedString.
type Bool bool

func (s Bool) String() string {
	if bool(s) {
		return `"True"^^<` + string(defaultBoolType) + `>`
	}
	return `"False"^^<` + string(defaultBoolType) + `>`
}
func (s Bool) Native() interface{} { return bool(s) }
func (s Bool) TypedString() TypedString {
	v := "False"
	if bool(s) {
		v = "True"
	}
	return TypedString{
		Value: String(v),
		Type:  defaultBoolType,
	}
}

var _ Equaler = Time{}

// Time is a native wrapper for time.Time type.
//
// It uses NQuad notation similar to TypedString.
type Time time.Time

func (s Time) String() string {
	return s.TypedString().String()
}
func (s Time) Native() interface{} { return time.Time(s) }
func (s Time) Equal(v Value) bool {
	t, ok := v.(Time)
	if !ok {
		return false
	}
	return time.Time(s).Equal(time.Time(t))
}
func (s Time) TypedString() TypedString {
	return TypedString{
		// TODO(dennwc): this is used to compute hash, thus we might want to include nanos
		Value: String(time.Time(s).Format(time.RFC3339)),
		Type:  defaultTimeType,
	}
}

type ByValueString []Value

func (o ByValueString) Len() int           { return len(o) }
func (o ByValueString) Less(i, j int) bool { return StringOf(o[i]) < StringOf(o[j]) }
func (o ByValueString) Swap(i, j int)      { o[i], o[j] = o[j], o[i] }

// Sequence is an object to generate a sequence of Blank Nodes.
type Sequence struct {
	last uint64
}

// Next returns a new blank node. It's safe for concurrent use.
func (s *Sequence) Next() BNode {
	n := atomic.AddUint64(&s.last, 1)
	return BNode(fmt.Sprintf("n%d", n))
}

var randSource = rand.New(rand.NewSource(time.Now().UnixNano()))

// RandomBlankNode returns a randomly generated Blank Node.
func RandomBlankNode() BNode {
	return BNode(fmt.Sprintf("n%d", randSource.Int()))
}
