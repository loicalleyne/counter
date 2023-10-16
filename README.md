# counter : increment/decrement metric counters and export them to an arrow record

## Overview

- Keys must be either string or integer, they will be concatenated into a key string for the underlying concurrent swiss map
- Metric counter is int64 - use Increment() or Decrement() to modify
- Export the metric results as an arrow record (ie. to write aggregated metrics to parquet)