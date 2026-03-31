# FlexQL: Detailed System Design Document

## 1. Introduction & Project Goals
FlexQL is a custom, high-performance SQL-like database driver and server implemented in C++17. The primary objective is to manage large-scale relational data (10M+ rows) with a minimal memory footprint while maintaining low-latency query execution.

### Architectural Pillars:
- **Scalability**: Capable of handling millions of rows without exceeding typical system RAM limits.
- **Persistence**: Row-major binary storage engine with crash-recovery awareness.
- **Concurrency**: Multithreaded TCP server supporting simultaneous client connections.
- **Performance**: Integrated B-Tree indexing and LRU query caching to minimize I/O.

---

## 2. Storage Engine Architecture

FlexQL employs a **Row-Major Binary Storage** model. This design was chosen for its alignment with OLTP (On-Line Transactional Processing) workloads, where entire records are frequently accessed or inserted.

### 2.1 Binary Serialization Format
Every row is serialized into a compact binary block in a `.dat` file:
```mermaid
graph LR
    A[Deleted Flag 1B] --> B[Expiry Time 8B]
    B --> C[Col 1 Data]
    C --> D[Col 2 Data]
    D --> E[Col N Data]
```
- **Fixed-width Columns**: `INT` (4B), `DECIMAL` (8B), and `DATETIME` (8B) are stored as raw bytes for $O(1)$ offset calculation within a known schema.
- **Variable-width Columns**: `VARCHAR` strings are prefixed by a `size_t` (8B on 64-bit) length, followed by the string content.

### 2.2 Performance Optimizations for 10M Rows
Handling 10 million rows (~700MB - 1GB depending on schema) requires active kernel page cache management to avoid **Out-Of-Memory (OOM)** failures.
- **Batch Serialization**: The `insertBatch` method serializes multiple rows into a single memory buffer before issuing a single `write()` system call. This amortizes the cost of context switching and disk seeks.
- **Page Cache Throttling**: Every 100,000 rows, the system invokes `fdatasync()` followed by `posix_fadvise(POSIX_FADV_DONTNEED)`. This forces the OS to flush dirty pages to disk and immediately marks them as reclaimable, keeping the process resident set size (RSS) low.
- **Zero-Copy Parsing**: The parser extracts values into a flat 1D array, which is then "moved" into the storage buffer, eliminating redundant string copies.

---

## 3. Indexing Subsystem

To avoid full table scans ($O(N)$), FlexQL implements a high-order **Internal B-Tree**.

### 3.1 B-Tree Implementation
- **Order 64**: Each node holds up to 63 keys and 64 child pointers, balancing tree depth against CPU cache line efficiency.
- **Primary Key Focus**: Indexes are built *only* for columns explicitly marked as `PRIMARY KEY`. This prevents memory bloat for non-essential columns.
- **Complexity**: Point lookups are reduced to $O(\log_b N)$, where $b=64$.

### 3.2 Memory Locality & Node Pooling
Instead of individual `new` allocations for every B-Tree node (which causes heap fragmentation), FlexQL uses a **Node Pool Allocator**. Nodes are stored in a contiguous-ish `std::deque`, ensuring that parent and child nodes are often spatially close in physical RAM, improving CPU cache hit rates during traversal.

---

## 4. SQL Parser & Execution Pipeline

FlexQL uses a hand-written **Recursive Descent Parser** for maximum control and performance.

### 4.1 Parser Design
The parser transforms raw SQL strings into **Statement Objects**:
1. **Lexical Analysis**: A Lexer converts the string into a stream of tokens (`SELECT`, `IDENTIFIER`, `NUMBER`, etc.).
2. **Syntactic Analysis**: The Parser consumes tokens and builds a `Statement` tree.
3. **Execution**: The `QueryExecutor` receives the statement and interacts with the `ColumnStore`.

### 4.2 Bulk Insert Optimization
For `INSERT INTO ... VALUES (...), (...), ...`, the parser avoids creating individual `Row` objects. Instead, it populates a **Flat 1D Vector** of `Value` objects. This significantly reduces heap allocations during the critical path of 10M-row bulk loads.

---

## 5. Caching & Networking

### 5.1 LRU Query Cache
A 4096-entry **Least Recently Used (LRU)** cache sits between the Network and Storage layers.
- **Keying**: Queries are hashed based on their full SQL string (e.g., `SELECT * FROM BIG_USERS WHERE ID = 500`).
- **Invalidation**: Any write operation (`INSERT`, `DELETE`) to a table triggers an immediate invalidation of all cached queries associated with that table name, ensuring ACID-like consistency for reads.

### 5.2 Multithreaded TCP Server
The server implementation uses a **Thread-per-Client** model:
- **Synchronization**: A `std::recursive_mutex` protects the shared `ColumnStore` and `Table` metadata.
- **Framing**: Because TCP is a stream protocol, FlexQL uses a custom delimiter `\n<EOF>\n` to identify the end of a query. This prevents "partial query" errors during high-throughput network bursts.

---

## 6. Maintenance & Reliability

### 6.1 Row Expiration (TTL)
Each row includes a `time_t` expiry field.
- **Lazy Check**: During a `SELECT` scan, rows with an expired timestamp are ignored.
- **Active Cleanup**: A background **Garbage Collection (GC)** thread wakes every 30 seconds to perform a pass over all tables, marking expired rows for deletion and freeing up index entries.

### 6.2 Crash Recovery
If the server terminates unexpectedly, the binary `.dat` file may contain a partially written row. Upon restart, the `ColumnStore` constructor scans the file. If an EOF is encountered before a row's defined length is satisfied, the rebuild stops at the last valid row, ensuring the database remains in a consistent state without requiring a complex WAL (Write-Ahead Log).

---