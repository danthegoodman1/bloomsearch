# Performance Testing

## Primitive test (Query Concurrency: 100)

M3 Max 128GB, 1.6GB dataset (~167MB per file * 10)

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
