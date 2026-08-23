# oracle-core

`oracle-core` is a lightweight TypeScript database abstraction for Oracle Database built on top of the [`oracledb`](https://www.npmjs.com/package/oracledb) driver.

It provides a small, consistent API for:

* Oracle connection pools
* Explicit transactions
* Query and scalar operations
* Batch execution
* Result mapping
* Boolean conversion
* Parameter normalization
* Optional object-to-JSON serialization

The library intentionally keeps transaction control explicit.

## Installation

```bash
npm install oracle-core oracledb
```

## Basic Usage

```ts
import oracledb from "oracledb"
import { OracleManager } from "oracle-core"

const pool = await oracledb.createPool({
  user: "scott",
  password: "tiger",
  connectString: "localhost/XEPDB1"
})

const db = new OracleManager(pool)

const users = await db.query<{
  id: number
  name: string
}>(
  "select id, name from users where status = :1",
  ["ACTIVE"]
)
```

## Oracle Parameters

Oracle uses `:n` positional parameters.

```ts
db.param(1) // :1
db.param(2) // :2
db.param(3) // :3
```

Example:

```ts
const user = await db.queryOne<User>(
  "select id, name from users where id = :1",
  [100]
)
```

## Transactions

Transactions use an explicit connection and explicit `commit()` / `rollback()`.

The recommended pattern is:

```ts
const tx = await db.beginTransaction()

try {
  await tx.execute(
    "update users set name = :1 where id = :2",
    ["John", 100]
  )

  await tx.execute(
    "update user_audit set updated_at = :1 where user_id = :2",
    [new Date(), 100]
  )

  await tx.commit()
} catch (err) {
  await tx.rollback()
  throw err
}
```

### Transaction lifecycle

A transaction owns one Oracle connection.

```text
db.beginTransaction()
        |
        v
OracleTransaction
        |
        +-- execute/query/...
        |
        +-- commit()   -> close connection
        |
        +-- rollback() -> close connection
```

After `commit()` or `rollback()`, the transaction is completed and cannot be used again.

```ts
await tx.commit()

await tx.execute("select 1 from dual")
// Error: Transaction has already been completed
```

### Failed commit

`commit()` marks the transaction as completed before attempting the Oracle commit and always closes the connection.

```ts
async commit(): Promise<void> {
  this.ensureActive()
  this.completed = true

  try {
    await this.con.commit()
  } finally {
    await this.con.close()
  }
}
```

This is intentional. If Oracle reports an error during `COMMIT`, the final database outcome may be uncertain. The transaction is therefore not reused or rolled back through the abstraction after `commit()` has been attempted.

## Important: Transaction Concurrency

A transaction represents a single Oracle connection.

Operations should be awaited sequentially:

```ts
await tx.execute(...)
await tx.query(...)
await tx.execute(...)
```

Do not execute multiple operations concurrently on the same transaction connection:

```ts
// Do not do this
await Promise.all([
  tx.execute(...),
  tx.execute(...),
  tx.query(...)
])
```

## Queries

### `query`

```ts
const users = await db.query<User>(
  "select id, name from users",
  []
)
```

Returns an array. An empty result produces `[]`.

### `queryOne`

```ts
const user = await db.queryOne<User>(
  "select id, name from users where id = :1",
  [100]
)
```

Returns:

```ts
User | null
```

### `executeScalar`

```ts
const total = await db.executeScalar<number>(
  "select count(*) from users"
)
```

Returns the first column of the first row, or `null`.

### `count`

```ts
const total = await db.count(
  "select count(*) from users"
)
```

Returns a number and converts `null` to `0`.

## `execute`

`execute()` executes a statement using a pooled connection.

```ts
await db.execute(
  "begin my_procedure(:1); end;",
  [100]
)
```

The normal `execute()` method does **not** explicitly commit.

For application DML that requires transaction control, use `beginTransaction()`:

```ts
const tx = await db.beginTransaction()

try {
  await tx.execute(
    "update users set active = :1 where id = :2",
    [1, 100]
  )

  await tx.commit()
} catch (err) {
  await tx.rollback()
  throw err
}
```

Do not assume that calling `db.execute()` by itself is equivalent to an explicit application transaction with commit/rollback semantics.

## Batch Execution

`executeBatch()` executes a group of statements as one internally managed transaction.

```ts
const affected = await db.executeBatch([
  {
    query: "insert into users(id, name) values (:1, :2)",
    params: [1, "Alice"]
  },
  {
    query: "insert into users(id, name) values (:1, :2)",
    params: [2, "Bob"]
  }
])
```

On success:

```text
statement 1
statement 2
statement 3
    |
    v
  COMMIT
```

If a statement fails:

```text
statement 1
statement 2
    |
    X
  ROLLBACK
```

### `requireFirstAffected`

When `requireFirstAffected` is `true`, the first statement controls whether the remaining statements execute.

```ts
const affected = await db.executeBatch(
  [
    {
      query: "update users set active = :1 where id = :2",
      params: [1, 100]
    },
    {
      query: "insert into user_log(user_id) values (:1)",
      params: [100]
    }
  ],
  true
)
```

The behavior is:

```text
First statement
      |
      +-- rowsAffected > 0 --> execute remaining statements
      |
      +-- rowsAffected = 0 --> stop and commit
```

This is useful when later operations depend on the first statement affecting a row.

## Batch Execution Inside an Existing Transaction

Use `executeBatchTx()` when the transaction is owned by the caller.

```ts
const tx = await db.beginTransaction()

try {
  await tx.executeBatch([
    {
      query: "update users set active = :1 where id = :2",
      params: [1, 100]
    },
    {
      query: "insert into user_log(user_id) values (:1)",
      params: [100]
    }
  ])

  await tx.commit()
} catch (err) {
  await tx.rollback()
  throw err
}
```

`executeBatchTx()`:

* does not commit
* does not rollback
* does not close the connection

The caller remains responsible for the transaction lifecycle.

## Statement

Batch statements use:

```ts
export interface Statement {
  query: string
  params?: any[]
}
```

Example:

```ts
const statements: Statement[] = [
  {
    query: "insert into users(id, name) values (:1, :2)",
    params: [1, "Alice"]
  },
  {
    query: "insert into users(id, name) values (:1, :2)",
    params: [2, "Bob"]
  }
]
```

## Result Mapping

Oracle column names can be mapped to application property names.

```ts
const users = await db.query<User>(
  "select USER_ID, USER_NAME from users",
  [],
  {
    USER_ID: "id",
    USER_NAME: "name"
  }
)
```

Result:

```ts
[
  {
    id: 1,
    name: "Alice"
  }
]
```

`query()` and `queryOne()` also support boolean conversion.

## Boolean Mapping

Oracle applications frequently represent booleans using values such as:

```text
1 / 0
Y / N
T / F
true / false
```

The library can convert these values to JavaScript booleans.

```ts
const users = await db.query<User>(
  "select id, enabled from users",
  [],
  undefined,
  [
    {
      name: "enabled"
    }
  ]
)
```

Without a custom mapping, the following values are treated as `true`:

```text
1
T
Y
true
```

A custom true value can be supplied:

```ts
{
  name: "enabled",
  true: "Y"
}
```

In that case only `"Y"` is treated as `true`.

## Parameter Normalization

The library normalizes parameters before sending them to Oracle.

```ts
undefined -> null
null      -> null
Date      -> Date
object    -> object
```

Example:

```ts
await db.query(
  "select * from users where deleted_at = :1",
  [undefined]
)
```

The undefined parameter is converted to `null`.

## Object Serialization

Objects can optionally be serialized using:

```ts
import { resource } from "oracle-core"

resource.string = true
```

Then object parameters are converted using `JSON.stringify()`.

Example:

```ts
resource.string = true

await db.execute(
  "insert into documents(id, data) values (:1, :2)",
  [1, { name: "Alice", active: true }]
)
```

The object parameter becomes a JSON string.

The default is:

```ts
resource.string = false
```

## Metadata-Based Result Conversion

Oracle query results are converted using Oracle metadata.

For example:

```sql
select
  USER_ID,
  USER_NAME
from users
```

is converted into objects using the metadata column names:

```ts
{
  USER_ID: 1,
  USER_NAME: "Alice"
}
```

The mapping parameter can then transform the property names into application naming conventions.

## Connection Management

`OracleManager` obtains connections from the configured `oracledb.Pool`.

Normal operations automatically close their connection when finished:

```text
OracleManager
    |
    +-- getConnection()
           |
           +-- execute/query
           |
           +-- close()
```

Transactions are different: the connection remains open until `commit()` or `rollback()`.

This ensures pooled connections are returned after normal operations and completed transactions.

## API

### `Executor`

```ts
export interface Executor {
  driver: string

  param(i: number): string

  execute(sql: string, args?: any[]): Promise<number>

  executeBatch(
    statements: Statement[],
    requireFirstAffected?: boolean
  ): Promise<number>

  query<T>(
    sql: string,
    args?: any[],
    m?: StringMap,
    bools?: Attribute[]
  ): Promise<T[]>

  queryOne<T>(
    sql: string,
    args?: any[],
    m?: StringMap,
    bools?: Attribute[]
  ): Promise<T | null>

  executeScalar<T>(
    sql: string,
    args?: any[]
  ): Promise<T | null>

  count(
    sql: string,
    args?: any[]
  ): Promise<number>
}
```

### `Transaction`

```ts
export interface Transaction extends Executor {
  commit(): Promise<void>
  rollback(): Promise<void>
}
```

### `DB`

```ts
export interface DB extends Executor {
  beginTransaction(): Promise<Transaction>
}
```

## Data Types

The library defines the following logical data types:

```ts
export type DataType =
  | "ObjectId"
  | "date"
  | "datetime"
  | "time"
  | "boolean"
  | "number"
  | "integer"
  | "string"
  | "text"
  | "object"
  | "array"
  | "binary"
  | "primitives"
  | "booleans"
  | "numbers"
  | "integers"
  | "strings"
  | "dates"
  | "datetimes"
  | "times"
```

Attributes are represented by:

```ts
export interface Attribute {
  name?: string
  column?: string
  type?: DataType
  default?: string | number | Date | boolean
  key?: boolean
  noinsert?: boolean
  noupdate?: boolean
  version?: boolean
  ignored?: boolean
  true?: string | number
  false?: string | number
}
```

## Design Philosophy

`oracle-core` intentionally provides a small abstraction over `oracledb`.

It does not attempt to hide Oracle's transaction model.

The preferred application pattern is:

```text
Normal read
    |
    +-- db.query()
    +-- db.queryOne()
    +-- db.executeScalar()
    +-- db.count()

Explicit transactional work
    |
    +-- db.beginTransaction()
           |
           +-- tx.execute()
           +-- tx.query()
           +-- tx.executeBatch()
           +-- tx.commit()
           +-- tx.rollback()

Self-contained batch
    |
    +-- db.executeBatch()
           |
           +-- internal transaction
```

This keeps transaction ownership explicit and predictable.

## License

MIT

# oracle-core

A lightweight TypeScript database abstraction and SQL builder for Oracle Database using [`oracledb`](https://www.npmjs.com/package/oracledb).

The library provides a consistent executor/transaction API, metadata-driven SQL generation, Oracle `MERGE`-based upsert support, batch operations, boolean conversion, version fields, and buffered file-import writers.

## Features

* TypeScript interfaces for database executors and transactions
* Oracle bind-parameter generation (`:1`, `:2`, ...)
* Metadata-driven SQL generation
* Single-row insert/upsert with Oracle `MERGE`
* Batch insert using Oracle `INSERT ALL`
* Batch save/upsert operations
* Transaction-aware batch execution
* `requireFirstAffected` execution flow
* Boolean value mapping
* Automatic version-column initialization and increment
* Optional object-to-string serialization
* Pluggable object mapping before persistence
* Buffered batch writer designed for sequential file imports
* Compatible with custom parameter builders

## Installation

```bash
npm install oracle-core
```

Install this package together with its peer/runtime requirements.

## Core Interfaces

### Executor

`Executor` defines the common database API:

```ts
export interface Executor {
  driver: string
  param(i: number): string
  execute(sql: string, args?: any[]): Promise<number>
  executeBatch(statements: Statement[], requireFirstAffected?: boolean): Promise<number>
  query<T>(sql: string, args?: any[], m?: StringMap, bools?: Attribute[]): Promise<T[]>
  queryOne<T>(sql: string, args?: any[], m?: StringMap, bools?: Attribute[]): Promise<T | null>
  executeScalar<T>(sql: string, args?: any[]): Promise<T | null>
  count(sql: string, args?: any[]): Promise<number>
}
```

### Transaction

```ts
export interface Transaction extends Executor {
  commit(): Promise<void>
  rollback(): Promise<void>
}
```

### DB

```ts
export interface DB extends Executor {
  beginTransaction(): Promise<Transaction>
}
```

## Parameter Builders

Oracle parameters use positional bind names:

```ts
param(1) // ":1"
param(2) // ":2"
param(3) // ":3"
```

Generate multiple parameters with:

```ts
params(3)
// [":1", ":2", ":3"]
```

A custom parameter builder can also be supplied to the SQL builders:

```ts
const stmt = buildToSave(
  user,
  "users",
  attributes,
  (i) => `:${i}`
)
```

## Attribute Metadata

Database mappings are defined using `Attribute` objects:

```ts
const attributes: Attributes = {
  id: {
    column: "ID",
    type: "number",
    key: true
  },

  name: {
    column: "NAME",
    type: "string"
  },

  active: {
    column: "ACTIVE",
    type: "boolean",
    true: 1,
    false: 0
  },

  version: {
    column: "VERSION",
    type: "integer",
    version: true
  }
}
```

Supported data types include:

```ts
type DataType =
  | "ObjectId"
  | "date"
  | "datetime"
  | "time"
  | "boolean"
  | "number"
  | "integer"
  | "string"
  | "text"
  | "object"
  | "array"
  | "binary"
  | "primitives"
  | "booleans"
  | "numbers"
  | "integers"
  | "strings"
  | "dates"
  | "datetimes"
  | "times"
```

### Attribute Options

| Option     | Description                                           |
| ---------- | ----------------------------------------------------- |
| `name`     | Runtime attribute name                                |
| `column`   | Database column name                                  |
| `type`     | Logical data type                                     |
| `default`  | Value used when an insert value is `null`/`undefined` |
| `key`      | Identifies a primary/business key used by upsert      |
| `noinsert` | Excludes the attribute from inserts                   |
| `noupdate` | Excludes the attribute from updates                   |
| `version`  | Marks the version column                              |
| `ignored`  | Excludes the attribute from persistence               |
| `true`     | Database representation of boolean `true`             |
| `false`    | Database representation of boolean `false`            |

## Metadata Generation

Create runtime metadata with:

```ts
const meta = metadata(attributes)
```

The result contains:

* key attributes
* boolean attributes
* column-to-property mappings
* version attribute
* non-ignored fields

## Insert Batch

`buildToInsertBatch()` creates an Oracle `INSERT ALL` statement.

```ts
const statement = buildToInsertBatch(
  users,
  "USERS",
  attributes
)
```

The generated SQL has the form:

```sql
insert all
  into USERS(ID,NAME,ACTIVE) values(:1,:2,:3)
  into USERS(ID,NAME,ACTIVE) values(:4,:5,:6)
select * from dual
```

This is useful when importing a collection of objects as one SQL operation.

## Save / Upsert

`buildToSave()` generates Oracle `MERGE` SQL.

```ts
const statement = buildToSave(
  user,
  "USERS",
  attributes
)
```

When all key fields are present, the generated SQL follows the pattern:

```sql
merge into USERS
using dual
on (ID=:1)
when matched then
  update set NAME=:2
when not matched then
  insert (ID,NAME)
  values (:1,:2)
```

When key values are absent, the builder generates an insert-oriented statement.

## Null and Undefined

Updates distinguish between `undefined` and `null`.

```ts
{
  name: undefined
}
```

means the column is not included in the update.

```ts
{
  name: null
}
```

generates:

```sql
NAME = null
```

For inserts, `null` and `undefined` fall back to the configured attribute default when one exists.

## Version Columns

A version attribute is initialized to `1` for new records:

```sql
VERSION = 1
```

For updates, the version is incremented:

```sql
VERSION = VERSION + 1
```

The version field therefore acts as a revision counter.

It does not by itself provide optimistic-lock conflict detection; the `MERGE` condition is based on the configured key fields.

## Boolean Mapping

Boolean properties can be mapped to Oracle-compatible values:

```ts
active: {
  type: "boolean",
  true: 1,
  false: 0
}
```

The following values are then bound:

```ts
true  → 1
false → 0
```

When no explicit mapping is supplied, the implementation uses:

```text
true  → 1
false → 0
```

## Execution

The low-level `execute()` helper executes a statement and returns `rowsAffected`:

```ts
const affected = await execute(
  connection,
  sql,
  params
)
```

The helper owns the supplied Oracle connection for this operation and closes it when execution finishes.

`execute()` is intended for direct driver-level execution where application-managed DML transactions are not required.

For transactional DML, use the Oracle driver/transaction APIs directly or the transaction-specific helpers.

## Transactions

### Execute and own the transaction

`executeBatch()` manages the transaction lifecycle:

```ts
const affected = await executeBatch(
  connection,
  statements
)
```

The operation:

1. Executes all statements with `autoCommit: false`
2. Commits when all statements succeed
3. Rolls back when an error occurs
4. Closes the connection

### Transaction-controlled execution

`executeBatchTx()` does not commit, rollback, or close the connection.

This allows the caller to manage the transaction:

```ts
try {
  await executeBatchTx(connection, statements)

  await connection.commit()
} catch (err) {
  await connection.rollback()
  throw err
}
```

## `requireFirstAffected`

Batch execution can optionally require the first statement to affect at least one row:

```ts
await executeBatch(
  connection,
  statements,
  true
)
```

When enabled:

```text
first statement affected rows > 0
    → execute the remaining statements

first statement affected rows = 0
    → stop execution
```

This is useful for dependent multi-step operations.

## Object Serialization

`toArray()` prepares parameters for Oracle execution.

`undefined` and `null` are converted to `null`.

Objects can optionally be serialized to JSON strings:

```ts
resource.string = true
```

Then:

```ts
{ name: "John", age: 20 }
```

is passed as:

```json
"{\"name\":\"John\",\"age\":20}"
```

When `resource.string` is not enabled, objects are passed through without JSON serialization.

## Writers

The library provides higher-level writer classes.

### OracleWriter

Writes one object at a time:

```ts
const writer = new OracleWriter(
  connection,
  "USERS",
  attributes
)

await writer.write(user)
```

An optional mapping function can transform the object before persistence.

`oneIfSuccess` can normalize the result to:

```text
0 = no rows affected
1 = one or more rows affected
```

### OracleBatchInserter

Optimized for bulk inserts:

```ts
const writer = new OracleBatchInserter(
  connection,
  "USERS",
  attributes
)

await writer.write(users)
```

It uses `INSERT ALL` rather than issuing one insert statement per object.

### OracleBatchWriter

Writes a collection using generated `MERGE` statements:

```ts
const writer = new OracleBatchWriter(
  connection,
  "USERS",
  attributes
)

await writer.write(users)
```

Each object becomes an individual `MERGE` statement executed as part of the batch transaction.

### OracleBufferedBatchWriter

Designed primarily for **sequential file-import workflows**.

```ts
const writer = new OracleBufferedBatchWriter(
  pool,
  "USERS",
  attributes,
  5000
)

for (const row of rows) {
  await writer.write(row)
}

await writer.flush()
```

Objects are accumulated until the configured batch size is reached.

The writer then:

1. Builds the batch
2. Obtains a connection from the pool
3. Executes the batch transaction
4. Clears the successfully written buffer

The class is intended for sequential import processing, where one writer instance is owned by one import operation.

## Mapping Imported Objects

All writer classes that support mapping accept a transformation function:

```ts
const writer = new OracleBatchWriter(
  connection,
  "USERS",
  attributes,
  (row) => ({
    ...row,
    active: row.status === "ACTIVE"
  })
)
```

This is useful when the input representation differs from the database representation.

## Numeric Values

`toString()` converts finite numbers to SQL numeric literals.

```ts
toString(123)       // "123"
toString(12.5)      // "12.5"
toString(NaN)       // "null"
toString(Infinity)  // "null"
```

Non-finite numeric values are represented as SQL `NULL`.

## Trusted Metadata

Table and column names are inserted directly into generated SQL:

```ts
table
attr.column
```

Therefore table and column metadata must come from trusted application configuration.

Values should always be supplied through bind parameters rather than string concatenation.

## Connection and Transaction Ownership

The execution helpers intentionally have different ownership models:

| Function           | Transaction         | Commit/Rollback                    | Close Connection |
| ------------------ | ------------------- | ---------------------------------- | ---------------- |
| `execute()`        | Direct execution    | No application-managed transaction | Yes              |
| `executeTx()`      | Caller controlled   | No                                 | No               |
| `executeBatch()`   | Function controlled | Yes                                | Yes              |
| `executeBatchTx()` | Caller controlled   | No                                 | No               |

This separation allows simple execution helpers and explicit transaction management to coexist.

## Design Philosophy

The library is intentionally lightweight.

It does not attempt to replace Oracle's native driver APIs. Instead, it provides:

```text
TypeScript abstraction
        +
metadata-driven SQL generation
        +
Oracle-specific execution helpers
        +
batch/import utilities
```

Applications can therefore use the high-level helpers for common persistence and import scenarios while retaining access to the native `oracledb` connection and transaction APIs when more advanced Oracle features are required.

## License

MIT
