# FlexQL design (interview walkthrough)

Repository: https://github.com/dishantvishwa302/FlexQL_Project

Say this in order. Stop after each section if they want to go deeper.

## 1. What it is (30 seconds)

A tiny SQL engine: TCP server, C client API, row-major disk files, B-tree on the primary key, LRU query cache. Language is C++17 only — no external database library.

One query path:

```
REPL  --flexql_exec-->  TCP  -->  Parser  -->  Executor
                                              |-- B-tree (PK)
                                              |-- full scan
                                              |-- hash join
                                              `-- LRU cache
```

## 2. Storage: row-major file

Each table is `data/tables/<name>.dat`. One row:

```
[deleted 1B] [expiry time_t] [col0] [col1] ... [colN]
```

INT / DECIMAL / DATETIME are fixed width. VARCHAR is `size_t length` + bytes.

Why row-major: INSERT and `SELECT *` want the whole record. A column store would win on `SELECT age FROM users`, which is not the workload here.

Rows are not kept in RAM. The process holds the B-tree and the cache. Inserts buffer many rows, then one `write()`. Every 1000 flushes: `fdatasync` + `posix_fadvise(DONTNEED)` so the kernel can drop page cache.

Schema is a sidecar `.schema` text file, reloaded when the server starts.

## 3. Index: B-tree of order 64

`PRIMARY KEY` on INT (or DECIMAL stored as int) builds a B-tree: key → byte offset in the `.dat` file.

- Node holds up to 127 keys (`2t - 1` with `t = 64`)
- Point lookup `WHERE id = 2` is O(log_64 n) — about three nodes for 10M rows
- Range `WHERE id > 10` walks the tree and returns offsets, then we read those rows

If the WHERE column is not the PK, we scan. That contrast is the whole point of the index.

Delete removes the key from the tree and sets the deleted flag in the file. We do not compact the file (explain: lazy deletion, same idea as the TTL GC).

## 4. Cache: LRU, 4096 entries

Key is the SELECT shape (table, columns, WHERE, ORDER BY, LIMIT). Value is the result text, capped at 1 MB.

INSERT / DELETE on a table drop every cache entry whose key contains that table name.

Demo: run the same SELECT twice and compare client-side time.

## 5. JOIN: hash join, not nested loops

`SELECT ... FROM A INNER JOIN B ON A.x = B.x`

1. Load both tables
2. Hash the right table: join-key string → list of rows
3. Probe with every left row

That is O(|A| + |B|) expected, not O(|A| × |B|). Optional WHERE is applied on the joined pair. Optional LIMIT is applied after ORDER BY.

## 6. Concurrency

Thread per client. The row file is behind `recursive_mutex`. Table map is behind a read/write lock. The cache has its own mutex.

Not a serializable MVCC engine — say that honestly. Shared-state locking is what the assignment asked for.

## 7. TTL

`INSERT ... WITH TTL 3` stores `now + 3` in the row header.

- Reads skip expired rows
- A background thread every 30 seconds marks them deleted and drops their B-tree keys

## 8. What I would not claim

- No WAL. A crash during `write()` can leave a truncated last row; restart stops at the last complete row.
- `SELECT *` on 10 million rows still materializes the result. Use `WHERE` on the PK or `LIMIT` in a demo.
- The B-tree lives in RAM. A disk B-tree would matter if the index itself did not fit in memory.
