package main

import (
	"fmt"
	"os"
	"time"

	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/compress"
	pq "github.com/apache/arrow/go/v14/parquet/pqarrow"
	"github.com/loicalleyne/counter"
)

func main() {
	layout := "2006-Jan-02"
	tm, _ := time.Parse(layout, "2014-Feb-04")
	ctr, _ := counter.NewCounter([]string{"event_time", "field1_id", "field2_id", "field3_id", "field4_name"}, []string{"T", "i", "i", "i", "S"}, "requests")
	ctr.Increment(1, tm, 39, 12345, 11, "HOST1")
	ctr.Increment(5, tm, 39, 12345, 11, "HOST2")
	ctr.Increment(7, tm, 39, 12345, 11, "HOST1")
	ctr.Increment(9, tm, 39, 12345, 11, "HOST3")
	ctr.Decrement(3, tm, 39, 12345, 11, "HOST3")
	m := ctr.Map()

	m.Range(func(key string, value int64) (stop bool) {
		fmt.Println(key, value)
		return false
	})

	pwProperties := parquet.NewWriterProperties(parquet.WithDictionaryDefault(true),
		parquet.WithVersion(parquet.V2_LATEST),
		parquet.WithCompression(compress.Codecs.Zstd),
	)
	awProperties := pq.NewArrowWriterProperties(pq.WithStoreSchema())

	fp, err := os.OpenFile("./counter.parquet", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	defer fp.Close()
	pr, err := pq.NewFileWriter(ctr.Schema, fp, pwProperties, awProperties)
	if err != nil {
		panic(err)
	}
	defer pr.Close()
	r, err := ctr.ArrowRec()
	if err != nil {
		panic(err)
	}
	err = pr.Write(*r)
	if err != nil {
		panic(err)
	}
}
