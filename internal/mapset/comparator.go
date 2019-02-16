package mapset

import (
	"time"

	"github.com/emirpasic/gods/utils"
)

var (
	ByteComparator   = utils.ByteComparator
	StringComparator = utils.StringComparator
	RuneComparator   = utils.RuneComparator
	TimeComparator   = utils.TimeComparator

	IntComparator   = utils.IntComparator
	Int8Comparator  = utils.Int8Comparator
	Int16Comparator = utils.Int16Comparator
	Int32Comparator = utils.Int32Comparator
	Int64Comparator = utils.Int64Comparator

	UIntComparator   = utils.UIntComparator
	UInt8Comparator  = utils.UInt8Comparator
	UInt16Comparator = utils.UInt16Comparator
	UInt32Comparator = utils.UInt32Comparator
	UInt64Comparator = utils.UInt64Comparator

	Float32Comparator = utils.Float32Comparator
	Float64Comparator = utils.Float64Comparator

	GenericComparator = genericComparator
)

func genericComparator(a, b interface{}) int {
	switch a.(type) {
	case string:
		return StringComparator(a, b)
	case rune:
		return RuneComparator(a, b)
	case []byte:
		return ByteComparator(a, b)
	case int:
		return IntComparator(a, b)
	case uint:
		return UIntComparator(a, b)
	case float32:
		return Float32Comparator(a, b)
	case float64:
		return Float64Comparator(a, b)
	case time.Time:
		return TimeComparator(a, b)
	}
	if a == b {
		return 0
	}
	return -2
	// panic(fmt.Sprintf("unknonw comparative type: %#v, %#v", a, b))
}
