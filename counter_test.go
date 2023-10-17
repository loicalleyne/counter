package counter

import (
	"testing"
	"time"
)

func TestIncrementDecrement(t *testing.T) {
	layout := "2006-Jan-02"
	tm, _ := time.Parse(layout, "2014-Feb-04")
	tests := []struct {
		fields     []string
		fieldTypes []string
		keys       []any
		metricName string
		time       string
	}{
		{
			fields:     []string{"event_time", "field1_id", "field2_id", "field3_id", "field4_name"},
			fieldTypes: []string{"t", "i", "i", "i", "s"},
			keys:       []any{tm, 39, 12345, 11, "STRING4"},
			metricName: "requests",
			time:       "2014-Feb-04",
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			ctr, _ := NewCounter(test.fields, test.fieldTypes, test.metricName)

			ctr.Increment(9, test.keys...)
			ctr.Decrement(3, test.keys...)
			value, err := ctr.Get(test.keys...)
			if err != nil {
				ks, _ := ctr.genKey(test.keys)
				t.Errorf("key %s not found", ks)
			}
			if value != 6 {
				t.Errorf("unexpected total %d, should be 6", value)
			}
		})
	}
}
