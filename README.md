# FlexQL: A Flexible SQL-like Database Driver

## Compilation & Execution

### Build Instructions
```bash
# Compile all binaries (Server, Client, Benchmark)
make clean && make -j4
```

### Running the System
1. **Start the Server**:
   ```bash
   ./bin/flexql_server
   ```
2. **Start the Client (REPL)**:
   ```bash
   ./bin/flexql_client
   ```
3. **Run Performance Benchmark**:
   ```bash
   ./bin/benchmark_flexql
   ```

---


## Features & SQL Support

FlexQL supports a robust subset of SQL functionality as defined in the requirements:

- **Data Types**: `INT`, `DECIMAL`, `VARCHAR`, `DATETIME`.
- **CREATE TABLE**: Full schema enforcement with Primary Key support.
- **INSERT**: High-speed batch insertions with row-level expiration (TTL).
- **SELECT**: Supports `SELECT *` and specific column selection.
- **WHERE Clause**: Single-condition filtering (e.g., `WHERE column = VALUE`).
- **INNER JOIN**: Efficient joining of two tables on a shared column.
- **TTL (Time-To-Live)**: Automatic row expiration handling.

---

## System Architecture & Design

### 1. Storage Layout (Row-Major Persistent)
Data is stored in binary `.dat` files under `data/tables/`. 
- **Serialization**: Rows are serialized as `[deleted_flag (1B) | expiry_time (8B) | column_data...]`.
- **Fixed vs Variable**: Numeric types (`INT`, `DECIMAL`, `DATETIME`) use fixed widths for fast offset calculation. `VARCHAR` uses length-prefixed encoding.
- **Persistence**: RAM is used primarily for caching and indexing; the disk remains the authoritative source of truth.

### 2. High-Performance Write Path
To handle 10 million rows without system crashes (OOM):
- **Batch Serialization**: Rows are buffered and written in chunks (e.g., 5,000 rows) using a single `write()` system call.
- **Memory Management**: Uses `fdatasync()` and `posix_fadvise` every 100K rows to force data to disk and reclaim kernel page cache, preventing RAM saturation.
- **Zero-Copy**: Move semantics are utilized during parsing to transfer data ownership directly to storage without redundant allocations.

### 3. Indexing Strategy
- **B-Tree**: An in-memory B-Tree (Order 64) maps Primary Keys to physical file offsets.
- **Lookup Complexity**: O(log N) for primary key queries.
- **Node Pooling**: A custom node allocator reduces heap fragmentation and improves CPU cache locality.

### 4. LRU Query Caching
- **Implementation**: A 4096-entry Least Recently Used (LRU) cache stores query results.
- **Invalidation**: Any mutation (`INSERT`, `DELETE`) automatically invalidates relevant cache entries to ensure data consistency.

### 5. Multi-threaded Server
- **Client Handling**: The server spawns a dedicated thread per client connection.
- **Concurrency Control**: Shared data structures are protected by `std::recursive_mutex` to ensure thread safety without deadlocks.
- **TCP Framing**: Custom protocol using `\n<EOF>\n` delimiters to handle TCP fragmentation for large multi-row packets.

### 6. Row Expiration (TTL)
- **Lazy Deletion**: Expired rows are filtered out during read operations.
- **Active GC**: A background Garbage Collector thread sweeps the database every 30 seconds to reclaim space from expired or deleted rows.

---

## API Specification

FlexQL provides a clean C/C++ API for application integration:

```cpp
// Open a connection to the server
int flexql_open(const char *host, int port, FlexQL **db);

// Execute an SQL statement
int flexql_exec(FlexQL *db, const char *sql, 
                int (*callback)(void*, int, char**, char**), 
                void *arg, char **errmsg);

// Close the connection and free resources
int flexql_close(FlexQL *db);

// Free memory allocated by the API (e.g., errmsg)
void flexql_free(void *ptr);
```

---


## Project Structure
```
flexql/
├── bin/            # Compiled binaries
├── build/          # Object files
├── data/           # Persistent storage (.dat files)
├── include/        # Header files
├── src/            # Source code
│   ├── client/     # Client REPL implementation
│   ├── server/     # Multithreaded server logic
│   ├── storage/    # Row-major storage engine
│   └── ...         # Index, Cache, Parser modules
└── Makefile        # Build system
```


## Performance Highlights :

Refer to `PERFORMANCE.md` file for highlights

---
