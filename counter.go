package counter

import (
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	csmap "github.com/mhmtszr/concurrent-swiss-map"
	"github.com/spf13/cast"
)

type Counter struct {
	dm           *csmap.CsMap[string, int64]
	fields       []string
	fieldTypes   []string
	shardCount   uint64
	size         uint64
	Schema       *arrow.Schema
	bld          *array.RecordBuilder
	separator    string
	timeFormat   string
	timeLocation *time.Location
}

// NewCounter receives the metric dimension field names, an []string indicating the type of the metric dimension fields,
// a variadic slice of option functions and returns a pointer to a new counter.
// Accepted dimension field types are :
//
//	"i","I","int64" : int64
//	"t","T","time" : time.Time
//	"s","S","string" : string
//
// Example:
// c, err := NewCounter([]string{"event_time", "entity1_name", "entity2_id"}, []string{"T", "S","I"}, "requests", WithTimeFormat(“02 Jan 06 15:04 -0700”))
func NewCounter(fields []string, ft []string, metricName string, options ...func(options *Counter) error) (*Counter, error) {
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
		case "i", "I", "int64":
			ft[idx] = "i"
			aFields = append(aFields, arrow.Field{Name: fields[idx], Type: arrow.PrimitiveTypes.Int64})
		case "t", "T", "time":
			ft[idx] = "t"
			aFields = append(aFields, arrow.Field{Name: fields[idx], Type: &arrow.TimestampType{Unit: arrow.Second}})
		case "s", "S", "string":
			ft[idx] = "s"
			aFields = append(aFields, arrow.Field{Name: fields[idx], Type: arrow.BinaryTypes.String})
		default:
			return nil, fmt.Errorf("invalid field type at position %d - %s", idx, t)
		}
	}
	aFields = append(aFields, arrow.Field{Name: metricName, Type: arrow.PrimitiveTypes.Int64})
	c := Counter{
		fields:       fields,
		fieldTypes:   ft,
		shardCount:   32,
		size:         64,
		separator:    "|",
		timeFormat:   "2006-01-02 15:04:05",
		timeLocation: time.UTC,
	}
	for _, option := range options {
		err := option(&c)
		if err != nil {
			return nil, fmt.Errorf("could not create counter - %w", err)
		}
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

func (c *Counter) Map() *csmap.CsMap[string, int64] { return c.dm }
func (c *Counter) Reset()                           { c.dm.Clear() }

func (c *Counter) ArrowRec() (*arrow.Record, error) {
	var e error
	c.dm.Range(func(key string, value int64) (stop bool) {
		keyParts := strings.Split(key, c.separator)
		for idx, fb := range c.bld.Fields() {
			if idx == len(c.fieldTypes) {
				fb.(*array.Int64Builder).Append(value)
			} else {
				switch c.fieldTypes[idx] {
				case "t", "T", "time":
					var err error
					ts, err := time.ParseInLocation(c.timeFormat, keyParts[idx], c.timeLocation)
					if err != nil {
						e = err
						return true
					}
					t, err := arrow.TimestampFromTime(ts, arrow.Second)
					if err != nil {
						e = err
						return true
					}
					fb.(*array.TimestampBuilder).Append(t)
				case "s", "S", "string":
					fb.(*array.StringBuilder).AppendString(keyParts[idx])
				case "i", "I", "int64":
					fb.(*array.Int64Builder).Append(cast.ToInt64(keyParts[idx]))
				}
			}
		}
		return false
	})
	if e != nil {
		return nil, e
	}
	r := c.bld.NewRecord()
	return &r, nil
}

func (c *Counter) Get(keys ...any) (int64, error) {
	if len(keys) != len(c.fields) {
		return 0, fmt.Errorf("%d keys provided, want %d", len(keys), len(c.fields))
	}
	ks, err := c.genKey(keys)
	if err != nil {
		return 0, err
	}
	m := c.dm
	s, _ := m.Load(ks)
	return s, nil
}

func (c *Counter) Delete(keys ...any) (bool, error) {
	if len(keys) != len(c.fields) {
		return false, fmt.Errorf("%d keys provided, want %d", len(keys), len(c.fields))
	}
	ks, err := c.genKey(keys)
	if err != nil {
		return false, err
	}
	m := c.dm
	d := m.Delete(ks)
	return d, nil
}

func (c *Counter) Increment(inc int64, keys ...any) error {
	if len(keys) != len(c.fields) {
		return fmt.Errorf("%d keys provided, want %d", len(keys), len(c.fields))
	}
	ks, err := c.genKey(keys)
	if err != nil {
		return err
	}
	m := c.dm
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
	ks, err := c.genKey(keys)
	if err != nil {
		return err
	}
	m := c.dm
	s, _ := m.Load(ks)
	if s == 0 {
		m.Store(ks, 0-dec)
	} else {
		s = s - dec
		m.Store(ks, s)
	}
	return nil
}

func WithShardCount(count uint64) func(counter *Counter) error {
	return func(counter *Counter) error {
		if count < 1 {
			return nil
		}
		counter.shardCount = count
		return nil
	}
}

func WithSize(size uint64) func(counter *Counter) error {
	return func(counter *Counter) error {
		if size < 1 {
			return nil
		}
		counter.size = size
		return nil
	}
}

func WithTimeFormat(f string) func(counter *Counter) error {
	return func(counter *Counter) error {
		if len(f) < 1 {
			return fmt.Errorf("empty time format string")
		}
		counter.timeFormat = f
		return nil
	}
}

func WithTimeLocation(f string) func(counter *Counter) error {
	return func(counter *Counter) error {
		if len(f) < 1 {
			return fmt.Errorf("empty time location string")
		}
		timeLoc, err := time.LoadLocation(f)
		if err != nil {
			return fmt.Errorf("time location - %w", err)
		}
		counter.timeLocation = timeLoc
		return nil
	}
}

func WithCustomSeparator(f string) func(counter *Counter) error {
	return func(counter *Counter) error {
		if len(f) < 1 {
			return fmt.Errorf("custom separator cannot be empty string")
		}
		counter.separator = f
		return nil
	}
}

func (c *Counter) genKey(keys []any) (string, error) {
	var ks string
	for idx, key := range keys {
		switch k := key.(type) {
		case time.Time:
			if c.fieldTypes[idx] != "t" {
				return "", fmt.Errorf("field type mismatch on %s", c.fields[idx])
			}
			if idx == 0 {
				ks = ks + k.Format(c.timeFormat)
			} else {
				ks = ks + c.separator + k.Format(c.timeFormat)
			}
		case string:
			if c.fieldTypes[idx] != "s" {
				return "", fmt.Errorf("field type mismatch on %s", c.fields[idx])
			}
			if idx == 0 {
				ks = ks + k
			} else {
				ks = ks + c.separator + k
			}
		case int64, int32, int16, int8, int:
			if c.fieldTypes[idx] != "i" {
				return "", fmt.Errorf("field type mismatch on %s", c.fields[idx])
			}
			if idx == 0 {
				ks = ks + cast.ToString(k)
			} else {
				ks = ks + c.separator + cast.ToString(k)
			}
		}
	}
	return ks, nil
}
