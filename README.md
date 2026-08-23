# FlexQL: A small SQL-like database you can explain in an interview

Client/server database in C++17. No SQLite, no Boost. The REPL talks to the server only through `flexql_open` / `flexql_exec` / `flexql_close`.

## 5-minute demo

```bash
make clean && make -j4

# terminal 1
./bin/flexql_server

# terminal 2
./bin/flexql_client
```

Then paste:

```sql
CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR, age INT);
INSERT INTO users VALUES (1, 'Alice', 22), (2, 'Bob', 30), (3, 'Carol', 25);
SELECT * FROM users;
SELECT * FROM users;
SELECT * FROM users WHERE id = 2;
SELECT name, age FROM users WHERE age > 23 ORDER BY age DESC;
SELECT * FROM users LIMIT 2;

CREATE TABLE orders (oid INT PRIMARY KEY, uid INT, amount DECIMAL);
INSERT INTO orders VALUES (10, 1, 50.5), (11, 1, 120), (12, 3, 9.99);
SELECT users.name, orders.amount
FROM users INNER JOIN orders ON users.id = orders.uid
WHERE orders.amount > 20;

INSERT INTO users VALUES (4, 'tmp', 0) WITH TTL 3;
SELECT * FROM users WHERE id = 4;
-- wait 3 seconds, then:
SELECT * FROM users WHERE id = 4;

DELETE FROM users WHERE id = 2;
```

Run the same `SELECT * FROM users;` twice. The second call should be faster — that is the LRU cache.

```bash
# optional: 1M-row insert benchmark (server must be running)
./bin/benchmark_flexql --unit-test
./bin/benchmark_flexql 1000000
```

## What it implements (assignment)

| Feature | How |
|---|---|
| CREATE TABLE | Schema on disk (`data/tables/<name>.schema`) |
| INSERT | Row-major `.dat` file, batched `write()` |
| SELECT / WHERE | One condition. PK equality uses the B-tree |
| INNER JOIN | Hash join on the `ON` column |
| Types | INT, DECIMAL, VARCHAR, DATETIME |
| Index | In-memory B-tree: INT PK → file offset |
| Cache | LRU of SELECT results, dropped on INSERT/DELETE |
| Threads | One thread per client + mutex on the file |
| TTL | Expiry timestamp per row; lazy skip + 30s GC |

## Interview extras (not required, useful to talk about)

- `ORDER BY` / `LIMIT` (LIMIT after ORDER BY, stop early on scans when there is no ORDER BY)
- `DELETE`
- `CREATE TABLE IF NOT EXISTS`
- `WITH TTL seconds`
- Schema reload on server restart

## Layout

```
include/     public C API + engine headers
src/client   REPL (uses flexql_* only)
src/server   TCP server
src/parser   lexer + recursive-descent parser
src/query    executor (select / insert / join / delete)
src/storage  row file + schema
src/index    B-tree
src/cache    LRU
src/common   types + C API
```

Design notes: `DESIGN_DOC.md`. Numbers from a previous 1M/10M run: `PERFORMANCE.md`.
