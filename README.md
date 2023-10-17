# counter : increment/decrement metric counters and export them to an arrow record

## Overview

- Field names and types must be specified at counter creation
- Keys must be of type integer, string or time.Time
- Metric counter is int64 
- Use Increment() or Decrement() to modify, Get() to retrieve and Delete() to remove from counter, Reset() to clear all
- Export the metric results as an arrow record (ie. to write aggregated metrics to parquet)
- Uses concurrent swiss map as underlying cache, thread safe