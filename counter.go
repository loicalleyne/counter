package counter

import (
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	csmap "github.com/mhmtszr/concurrent-swiss-map"
	"github.com/spf13/cast"
)

type Counter struct {
	dm         *csmap.CsMap[string, int64]
	fields     []string
	fieldTypes []arrow.DataType
	shardCount uint64
	size       uint64
	Schema     *arrow.Schema
	bld        *array.RecordBuilder
	separator  string
}

func (c *Counter) Map() *csmap.CsMap[string, int64] { return c.dm }
func (c *Counter) Reset()                           { c.dm.Clear() }

func (c *Counter) ArrowRec() *arrow.Record {
	c.dm.Range(func(key string, value int64) (stop bool) {
		keyParts := strings.Split(key, c.separator)
		for idx, fb := range c.bld.Fields() {
			if idx == len(c.fieldTypes) {
				fb.(*array.Int64Builder).Append(value)
			} else {
				switch c.fieldTypes[idx] {
				case arrow.BinaryTypes.String:
					fb.(*array.StringBuilder).AppendString(keyParts[idx])
				case arrow.PrimitiveTypes.Int64:
					fb.(*array.Int64Builder).Append(cast.ToInt64(keyParts[idx]))
				}
			}
		}
		return false
	})
	r := c.bld.NewRecord()
	return &r
}

func NewCounter(fields []string, metric string, ft []arrow.DataType, options ...func(options *Counter)) (*Counter, error) {
	if len(fields) < 1 {
		return nil, fmt.Errorf("no field names provided")
	}
	if len(ft) < 1 {
		return nil, fmt.Errorf("no field types provided")
	}
	if len(fields) != len(ft) {
		return nil, fmt.Errorf("number of fields and number of field types mismatch")
	}
	var aFields []arrow.Field

	for idx, t := range ft {
		switch t {
		case arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64:
			aFields = append(aFields, arrow.Field{Name: fields[idx], Type: t})
		default:
			return nil, fmt.Errorf("invalid field type at position %d - %s", idx, t.String())
		}
	}
	aFields = append(aFields, arrow.Field{Name: metric, Type: arrow.PrimitiveTypes.Int64})
	c := Counter{
		fields:     fields,
		fieldTypes: ft,
		shardCount: 32,
		size:       64,
		separator:  "|",
	}
	for _, option := range options {
		option(&c)
	}
	dm := csmap.Create[string, int64](
		csmap.WithShardCount[string, int64](c.shardCount),
		csmap.WithSize[string, int64](c.size),
	)
	c.Schema = arrow.NewSchema(aFields, nil)
	c.bld = array.NewRecordBuilder(memory.DefaultAllocator, c.Schema)
	c.dm = dm
	return &c, nil
}

func (c *Counter) Increment(inc int64, keys ...any) error {
	if len(keys) != len(c.fields) {
		return fmt.Errorf("%d keys provided, want %d", len(keys), len(c.fields))
	}
	var ks string
	m := c.dm
	for idx, key := range keys {
		switch k := key.(type) {
		case string:
			if idx == 0 {
				ks = ks + k
			} else {
				ks = ks + c.separator + k
			}
		case int64, int32, int16, int8, int:
			if idx == 0 {
				ks = ks + cast.ToString(k)
			} else {
				ks = ks + c.separator + cast.ToString(k)
			}
		}
	}
	s, _ := m.Load(ks)
	if s == 0 {
		m.Store(ks, inc)
	} else {
		s = s + inc
		m.Store(ks, s)
	}
	return nil
}

func (c *Counter) Decrement(dec int64, keys ...any) error {
	if len(keys) != len(c.fields) {
		return fmt.Errorf("%d keys provided, want %d", len(keys), len(c.fields))
	}
	var ks string
	m := c.dm
	for idx, key := range keys {
		switch k := key.(type) {
		case string:
			if idx == 0 {
				ks = ks + k
			} else {
				ks = ks + c.separator + k
			}
		case int64, int32, int16, int8, int:
			if idx == 0 {
				ks = ks + cast.ToString(k)
			} else {
				ks = ks + c.separator + cast.ToString(k)
			}
		}
	}
	s, _ := m.Load(ks)
	if s == 0 {
		m.Store(ks, 0-dec)
	} else {
		s = s - dec
		m.Store(ks, s)
	}
	return nil
}

func WithShardCount(count uint64) func(counter *Counter) {
	return func(counter *Counter) {
		if count < 1 {
			return
		}
		counter.shardCount = count
	}
}

func WithSize(size uint64) func(counter *Counter) {
	return func(counter *Counter) {
		if size < 1 {
			return
		}
		counter.size = size
	}
}
