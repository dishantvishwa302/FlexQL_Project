# Performance notes

These numbers are from a previous run of `./bin/benchmark_flexql` against a local server. Re-run if you need fresh figures for a talk.

## 1 million rows

| Metric | Value |
| :--- | :--- |
| Rows | 1,000,000 |
| Insert time | 3,972 ms |
| Throughput | ~251,762 rows/s |
| Unit tests | 22/22 |

## 10 million rows

| Metric | Value |
| :--- | :--- |
| Rows | 10,000,000 |
| Insert time | 65,825 ms |
| Throughput | ~151,917 rows/s |
| Unit tests | 22/22 |

What the benchmark actually stresses: batched INSERT over the C API, then a small SQL unit-test set (WHERE, ORDER BY, INNER JOIN). It does not `SELECT *` the full 10M table.
