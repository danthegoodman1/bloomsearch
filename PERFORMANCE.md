# Performance Testing

I've explored using:

1. Per-file concurrency instead of per-block
2. Alterantive JSON serialization
3. Buffered IO for the FileStore implementation

And did not see any performance gains outside of error, often seeing reduced performance.

## Primitive test

Uncompressed, query concurrency 100, M3 Max 128GB, 1.6GB dataset (~167MB per file * 10)

Random 00-09 partitions, no minmax indexes, testing FileStore for DataStore + MetaStore

### Token search

Search for `apple` as a token in any field:

```
=== Query Results: token_match ===
Total query time: 649.983542ms
Blocks processed: 100
Total rows processed: 11214340
Total bytes processed: 1.6 GB
Results returned: 1
System throughput: 17080402 rows/sec, 2.5 GB/sec
Concurrency factor: 51.3x (combined worker time: 33.342631585s)
Query selectivity: 0.00% (1 results / 11214340 rows)

--- Found Results ---
Result 1: {
  "SbdXwyPEKen": [
    "UdoPpw"
  ],
  "b9DVOMloi": "aPplE", <-- "apple" when lowercase
  "loJ7iQtrw": [
    "H41F3mi",
    "Xr9J",
    "n0DQx"
  ]
}
```

### Field search

Searching for field `SbdXwyPEKen`

```
=== Query Results: field_match ===
Total query time: 638.450959ms
Blocks processed: 100
Total rows processed: 11214340
Total bytes processed: 1.6 GB
Results returned: 1
System throughput: 17564920 rows/sec, 2.6 GB/sec
Concurrency factor: 55.6x (combined worker time: 35.488950715s)
Query selectivity: 0.00% (1 results / 11214340 rows)

--- Found Results ---
Result 1: {
  "SbdXwyPEKen": [
    "UdoPpw"
  ],
  "b9DVOMloi": "aPplE",
  "loJ7iQtrw": [
    "H41F3mi",
    "Xr9J",
    "n0DQx"
  ]
}
```

### Field:Token search

Search for `"b9DVOMloi": "aPplE"` field:token

```
=== Query Results: field_token_match ===
Total query time: 667.9685ms
Blocks processed: 100
Total rows processed: 11214340
  Block test_dat[123364032]: 187677.8 rows/s, 28.1 MB/s bytes/s, 112187 rows, 597.763792ms skipped=true
Total bytes processed: 1.6 GB
Results returned: 1
System throughput: 16788726 rows/sec, 2.5 GB/sec
Concurrency factor: 53.0x (combined worker time: 35.428038502s)
Query selectivity: 0.00% (1 results / 11214340 rows)

--- Found Results ---
Result 1: {
  "SbdXwyPEKen": [
    "UdoPpw"
  ],
  "b9DVOMloi": "aPplE",
  "loJ7iQtrw": [
    "H41F3mi",
    "Xr9J",
    "n0DQx"
  ]
}
```

### Throughput during flush

Because flushing is handled by a dedicated goroutine, there is zero impact on ingestion rate:

```
Batch 446000: Generated 395.4 MB (38.6%), throughput: 13.4 MB/s
Batch 448000: Generated 397.1 MB (38.8%), throughput: 13.4 MB/s
FLUSH TRIGGER: Partition '04' hit max bytes (10485976 >= 10485760)
FLUSH STARTING: 10 partitions, 1124400 total rows, 104504550 total bytes
  Partition '07': 112079 rows, 10423227 bytes
  Partition '00': 112185 rows, 10436494 bytes
  Partition '02': 112827 rows, 10483631 bytes
  Partition '01': 112149 rows, 10426135 bytes
  Partition '05': 112386 rows, 10432702 bytes
  Partition '09': 112699 rows, 10479087 bytes
  Partition '06': 112284 rows, 10436478 bytes
  Partition '04': 112813 rows, 10485976 bytes
  Partition '08': 112412 rows, 10425104 bytes
  Partition '03': 112566 rows, 10475716 bytes
Batch 450000: Generated 398.9 MB (39.0%), throughput: 13.4 MB/s
Batch 452000: Generated 400.7 MB (39.1%), throughput: 13.4 MB/s
```

## Primitive test (Snappy)

Snappy query concurrency 100, M3 Max 128GB, 1.8GB dataset (~186MB per file * 10)

### Token search ("taco")

```
=== Query Results: token_match ===
Total query time: 728.656042ms
Blocks processed: 100
Total rows processed: 11212020
Total bytes processed: 1.8 GB
Results returned: 6
System throughput: 15387260 rows/sec, 2.5 GB/sec
Peak worker throughput: 958261 rows/sec, 158.1 MB/sec
Concurrency factor: 51.2x (combined worker time: 37.334941618s)
Query selectivity: 0.00% (6 results / 11212020 rows)

--- Found Results ---
Result 1: {
  "9kO": [
    "sF1l",
    "3wt3xAN"
  ],
  "MuSA53GrAf": "kcCZgBYEXGd",
  "PjzOzduLekEp": "sGBvG1S8d",
  "raB": [
    "UEXOUwNOtz"
  ],
  "uUB1": [
    "FUuj9PSs",
    "TAcO",
    "IhSZXh"
  ]
}
Result 2: {
  "3jal": [
    "1l1pgZX5",
    "QTQ"
  ],
  "9QFcEIL2LsEC": [
    "xAjP1jW8",
    "XQp",
    "esYtAu8jQ"
  ],
  "Jc7E16nwFM": "6OMuS",
  "pYxfh7wt": [
    "TaCO"
  ]
}
Result 3: {
  "9mxnqrTj": [
    "Qc8Dmwy7",
    "01G5",
    "AqfM"
  ],
  "DL5lI0nPA": [
    "5oqO"
  ],
  "IVdolTU2ixm": [
    "TAcO"
  ]
}
Result 4: {
  "1DeVxs": "zCq8Rj207iU",
  "ZcXcbv9": [
    "TACO",
    "OPKQUBnB8I0"
  ]
}
Result 5: {
  "9GI0SGLt": "UneTkafr",
  "Bx2": [
    "SzR6zd7",
    "ztKUq",
    "NhfYFDz"
  ],
  "JZTTuGuBz": [
    "tACO",
    "I0I4RTvXB",
    "wRV8NLFieWoI"
  ],
  "UzvfVoa0xA3c": [
    "EwcVU6ybN"
  ]
}
Result 6: {
  "PfSrwgFMz3SU": [
    "b3oZOGs",
    "Hr4",
    "3IAxepeyG9V9"
  ],
  "RnOgyeK0k8qd": [
    "LAOdmZB93zi",
    "RNj",
    "UY7NL8eD"
  ],
  "SMkpswQ8RMZ": [
    "GKZWfnETM"
  ],
  "kzL": "SFkhlt",
  "o6tVhfN9": "taco"
}
✅ Query token_match completed successfully
```

### Field search ("SMkpswQ8RMZ")

```
=== Query Results: field_match ===
Total query time: 656.878459ms
Blocks processed: 100
Total rows processed: 11212020
Total bytes processed: 1.8 GB
Results returned: 1
System throughput: 17068637 rows/sec, 2.8 GB/sec
Peak worker throughput: 1321213 rows/sec, 217.8 MB/sec
Concurrency factor: 53.5x (combined worker time: 35.128201959s)
Query selectivity: 0.00% (1 results / 11212020 rows)

--- Found Results ---
Result 1: {
  "PfSrwgFMz3SU": [
    "b3oZOGs",
    "Hr4",
    "3IAxepeyG9V9"
  ],
  "RnOgyeK0k8qd": [
    "LAOdmZB93zi",
    "RNj",
    "UY7NL8eD"
  ],
  "SMkpswQ8RMZ": [
    "GKZWfnETM"
  ],
  "kzL": "SFkhlt",
  "o6tVhfN9": "taco"
}
✅ Query field_match completed successfully
```


### Field Token `.FieldToken("SMkpswQ8RMZ", "gkzwfnetm")`

```
=== Query Results: field_token_match ===
Total query time: 702.44675ms
Blocks processed: 100
Total rows processed: 11212020
Total bytes processed: 1.8 GB
Results returned: 1
System throughput: 15961381 rows/sec, 2.6 GB/sec
Peak worker throughput: 910399 rows/sec, 150.1 MB/sec
Concurrency factor: 48.6x (combined worker time: 34.138204917s)
Query selectivity: 0.00% (1 results / 11212020 rows)

--- Found Results ---
Result 1: {
  "PfSrwgFMz3SU": [
    "b3oZOGs",
    "Hr4",
    "3IAxepeyG9V9"
  ],
  "RnOgyeK0k8qd": [
    "LAOdmZB93zi",
    "RNj",
    "UY7NL8eD"
  ],
  "SMkpswQ8RMZ": [
    "GKZWfnETM"
  ],
  "kzL": "SFkhlt",
  "o6tVhfN9": "taco"
}
✅ Query field_token_match completed successfully
```

## Primitive test (ZSTD)

ZSTD compression level 1, query concurrency 100, M3 Max 128GB, 1.3GB dataset (~137.6MB per file * 10)

Random 00-09 partitions, no minmax indexes, testing FileStore for DataStore + MetaStore

### Field search

Searching for field `NOVNKQcRlOF5`

```
=== Query Results: field_match ===
Total query time: 618.642042ms
Blocks processed: 100
Total rows processed: 11215240
Total bytes processed: 1.3 GB
Results returned: 1
System throughput: 18128803 rows/sec, 2.2 GB/sec
Peak worker throughput: 1440506 rows/sec, 177.0 MB/sec
Concurrency factor: 52.9x (combined worker time: 32.696153084s)
Query selectivity: 0.00% (1 results / 11215240 rows)

--- Found Results ---
Result 1: {
  "NOVNKQcRlOF5": "T8iJte",
  "Qcunk": [
    "VlH7V",
    "mjxyN"
  ],
  "d8OwE": [
    "uj5C3Cx4EDGX"
  ],
  "lMS5d9me": "jm8hz",
  "ugS": [
    "3WHJfFLG",
    "TAcO"
  ]
}
```

### Token search

Search for `t8ijte` as a token in any field:

```
=== Query Results: token_match ===
Total query time: 639.600917ms
Blocks processed: 100
Total rows processed: 11215240
Total bytes processed: 1.3 GB
Results returned: 1
System throughput: 17534747 rows/sec, 2.1 GB/sec
Peak worker throughput: 1438881 rows/sec, 176.1 MB/sec
Concurrency factor: 50.0x (combined worker time: 32.004213128s)
Query selectivity: 0.00% (1 results / 11215240 rows)

--- Found Results ---
Result 1: {
  "NOVNKQcRlOF5": "T8iJte",
  "Qcunk": [
    "VlH7V",
    "mjxyN"
  ],
  "d8OwE": [
    "uj5C3Cx4EDGX"
  ],
  "lMS5d9me": "jm8hz",
  "ugS": [
    "3WHJfFLG",
    "TAcO"
  ]
}
```

### Field:Token search

Search for `"NOVNKQcRlOF5": "t8ijte"` field:token

```
=== Query Results: field_token_match ===
Total query time: 639.403ms
Blocks processed: 100
Total rows processed: 11215240
Total bytes processed: 1.3 GB
Results returned: 1
System throughput: 17540174 rows/sec, 2.1 GB/sec
Peak worker throughput: 990409 rows/sec, 121.4 MB/sec
Concurrency factor: 51.4x (combined worker time: 32.881143075s)
Query selectivity: 0.00% (1 results / 11215240 rows)

--- Found Results ---
Result 1: {
  "NOVNKQcRlOF5": "T8iJte",
  "Qcunk": [
    "VlH7V",
    "mjxyN"
  ],
  "d8OwE": [
    "uj5C3Cx4EDGX"
  ],
  "lMS5d9me": "jm8hz",
  "ugS": [
    "3WHJfFLG",
    "TAcO"
  ]
}
```
