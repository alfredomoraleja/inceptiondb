# MySQL Interface SQL Coverage

The MySQL wire protocol interface aims to feel familiar to SQL users. The table below collects typical statements we would like to support and their current status.

| Statement | Example | Status |
|-----------|---------|--------|
| `SELECT`  | `SELECT name FROM people WHERE age = 30` | Supported |
| `INSERT`  | `INSERT INTO people VALUES ('{"name":"Alice"}')` | Supported |
| `DELETE`  | `DELETE FROM people WHERE id = '123'` | Supported |
| `UPDATE`  | `UPDATE people SET age = 31 WHERE id = '123'` | Supported |
| `REPLACE` | `REPLACE INTO people VALUES ('{"id":"123","name":"Alice"}')` | Supported |
| `UPSERT`  | `INSERT INTO people VALUES ('{"id":"123"}') ON DUPLICATE KEY UPDATE name = 'Alice'` | Supported |
| `SHOW COLLECTIONS` | `SHOW COLLECTIONS` | Supported |
| `CREATE COLLECTION` | `CREATE COLLECTION people` | Supported |
| `DROP COLLECTION` | `DROP COLLECTION people` | Supported |

The list is not exhaustive, but it captures the baseline SQL vocabulary we expect client tools to issue when interacting with document collections. When implementing a new statement, add an example here so we can keep track of coverage.
