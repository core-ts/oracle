import { Connection, Metadata, Pool } from "oracledb"
import { buildToInsertBatch, buildToSave, buildToSaveBatch, param } from "./build"
import { Attribute, Attributes, DB, Statement, StringMap, Transaction } from "./metadata"

export * from "./build"
export * from "./checker"
export * from "./metadata"

// OracleDB.autoCommit = true;

// tslint:disable-next-line:class-name
export class resource {
  static string?: boolean
}
export class OracleTransaction implements Transaction {
  constructor(protected con: Connection) {
    this.param = this.param.bind(this)
    this.execute = this.execute.bind(this)
    this.executeBatch = this.executeBatch.bind(this)
    this.query = this.query.bind(this)
    this.queryOne = this.queryOne.bind(this)
    this.executeScalar = this.executeScalar.bind(this)
    this.count = this.count.bind(this)
    this.ensureActive = this.ensureActive.bind(this)
  }
  private completed = false
  private ensureActive(): void {
  if (this.completed) {
    throw new Error("Transaction has already been completed")
  }
}
  async commit(): Promise<void> {
    this.ensureActive()
    this.completed = true
    try {
      await this.con.commit()
    } finally {
      await this.con.close()
    }
  }
  async rollback(): Promise<void> {
    this.ensureActive()
    this.completed = true
    try {
      await this.con.rollback()
    } finally {
      await this.con.close()
    }
  }
  driver = "oracle"
  param(i: number): string {
    return ":" + i
  }
  execute(sql: string, args?: any[], ctx?: any): Promise<number> {
    this.ensureActive()
    const p = ctx ? ctx : this.con
    return executeTx(p, sql, args)
  }
  executeBatch(statements: Statement[], firstSuccess?: boolean, ctx?: any): Promise<number> {
    this.ensureActive()
    const p = ctx ? ctx : this.con
    return executeBatchTx(p, statements, firstSuccess)
  }
  query<T>(sql: string, args?: any[], m?: StringMap, bools?: Attribute[], ctx?: any): Promise<T[]> {
    this.ensureActive()
    const p = ctx ? ctx : this.con
    return queryTx(p, sql, args, m, bools)
  }
  queryOne<T>(sql: string, args?: any[], m?: StringMap, bools?: Attribute[], ctx?: any): Promise<T | null> {
    this.ensureActive()
    const p = ctx ? ctx : this.con
    return queryOneTx(p, sql, args, m, bools)
  }
  executeScalar<T>(sql: string, args?: any[], ctx?: any): Promise<T | null> {
    this.ensureActive()
    const p = ctx ? ctx : this.con
    return executeScalarTx<T>(p, sql, args)
  }
  count(sql: string, args?: any[], ctx?: any): Promise<number> {
    this.ensureActive()
    const p = ctx ? ctx : this.con
    return countTx(p, sql, args)
  }
}
export class OracleManager implements DB {
  constructor(protected pool: Pool) {
    this.param = this.param.bind(this)
    this.execute = this.execute.bind(this)
    this.executeBatch = this.executeBatch.bind(this)
    this.query = this.query.bind(this)
    this.queryOne = this.queryOne.bind(this)
    this.executeScalar = this.executeScalar.bind(this)
    this.count = this.count.bind(this)
  }
  async beginTransaction(): Promise<Transaction> {
    const connection = await this.pool.getConnection()
    const tx = new OracleTransaction(connection)
    return tx
  }
  driver = "oracle"
  param(i: number): string {
    return ":" + i
  }
  execute(sql: string, args?: any[], ctx?: any): Promise<number> {
    if (ctx) {
      return execute(ctx, sql, args)
    } else {
      return this.pool.getConnection().then((con) => execute(con, sql, args))
    }
  }
  executeBatch(statements: Statement[], firstSuccess?: boolean, ctx?: any): Promise<number> {
    if (ctx) {
      return executeBatch(ctx, statements, firstSuccess)
    } else {
      return this.pool.getConnection().then((con) => executeBatch(con, statements, firstSuccess))
    }
  }
  query<T>(sql: string, args?: any[], m?: StringMap, bools?: Attribute[], ctx?: any): Promise<T[]> {
    if (ctx) {
      return query<T>(ctx, sql, args, m, bools)
    } else {
      return this.pool.getConnection().then((con) => query<T>(con, sql, args, m, bools))
    }
  }
  queryOne<T>(sql: string, args?: any[], m?: StringMap, bools?: Attribute[], ctx?: any): Promise<T | null> {
    if (ctx) {
      return queryOne<T>(ctx, sql, args, m, bools)
    } else {
      return this.pool.getConnection().then((con) => queryOne<T>(con, sql, args, m, bools))
    }
  }
  executeScalar<T>(sql: string, args?: any[], ctx?: any): Promise<T | null> {
    if (ctx) {
      return executeScalar(ctx, sql, args)
    } else {
      return this.pool.getConnection().then((con) => executeScalar(con, sql, args))
    }
  }
  count(sql: string, args?: any[], ctx?: any): Promise<number> {
    if (ctx) {
      return count(ctx, sql, args)
    } else {
      return this.pool.getConnection().then((con) => count(con, sql, args))
    }
  }
}

export async function executeBatch(con: Connection, statements: Statement[], firstSuccess?: boolean): Promise<number> {
  if (!statements || statements.length === 0) {
    return 0
  } else if (statements.length === 1) {
    return execute(con, statements[0].query, statements[0].params)
  }
  let c = 0
  try {
    if (firstSuccess) {
      const result0 = await con.execute(statements[0].query, statements[0].params as any, { autoCommit: false })
      if (result0 && result0.rowsAffected && result0.rowsAffected > 0) {
        c += result0.rowsAffected
        const l = statements.length
        for (let j = 1; j < l; j++) {
          const item = statements[j]
          const res = await con.execute(item.query, item.params ? item.params : [], { autoCommit: false })
          if (res.rowsAffected) {
            c += res.rowsAffected
          }
        }
        await con.commit()
        return c
      } else {
        await con.commit()
        return c
      }
    } else {
      const l = statements.length
      for (let j = 0; j < l; j++) {
        const item = statements[j]
        const res = await con.execute(item.query, item.params ? item.params : [], { autoCommit: false })
        if (res.rowsAffected) {
          c += res.rowsAffected
        }
      }
      await con.commit()
      return c
    }
  } catch (e) {
    await con.rollback()
    // console.log(e);
    throw e
  } finally {
    await con.close()
  }
}
export async function executeBatchTx(con: Connection, statements: Statement[], firstSuccess?: boolean): Promise<number> {
  if (!statements || statements.length === 0) {
    return 0
  } else if (statements.length === 1) {
    return executeTx(con, statements[0].query, statements[0].params)
  }
  let c = 0
  try {
    if (firstSuccess) {
      const result0 = await con.execute(statements[0].query, statements[0].params as any, { autoCommit: false })
      if (result0 && result0.rowsAffected && result0.rowsAffected > 0) {
        c += result0.rowsAffected
        const l = statements.length
        for (let j = 1; j < l; j++) {
          const item = statements[j]
          const res = await con.execute(item.query, item.params ? item.params : [], { autoCommit: false })
          if (res.rowsAffected) {
            c += res.rowsAffected
          }
        }
        return c
      } else {
        return c
      }
    } else {
      const l = statements.length
      for (let j = 0; j < l; j++) {
        const item = statements[j]
        const res = await con.execute(item.query, item.params ? item.params : [], { autoCommit: false })
        if (res.rowsAffected) {
          c += res.rowsAffected
        }
      }
      return c
    }
  } catch (e) {
    // console.log(e);
    throw e
  }
}

export function executeTx(con: Connection, sql: string, args?: any[]): Promise<number> {
  const p = toArray(args)
  return con.execute(sql, p, { autoCommit: false }).then((results) => results.rowsAffected ?? 0)
}
export function queryTx<T>(con: Connection, sql: string, args?: any[], m?: StringMap, bools?: Attribute[]): Promise<T[]> {
  const p = toArray(args)
  return con.execute<T>(sql, p, { autoCommit: false }).then((results) => {
    if (results.rows) {
      const x = results.metaData
      if (!x) {
        return results.rows
      } else {
        const arrayResult = results.rows.map((item) => {
          return formatData<T>(x, item)
        })
        return handleResults(arrayResult, m, bools)
      }
    } else {
      return []
    }
  })
}
export function queryOneTx<T>(con: Connection, sql: string, args?: any[], m?: StringMap, bools?: Attribute[]): Promise<T | null> {
  return queryTx<T>(con, sql, args, m, bools).then((r) => {
    return r && r.length > 0 ? r[0] : null
  })
}
export function executeScalarTx<T>(con: Connection, sql: string, args?: any[]): Promise<T | null> {
  return queryOneTx<T>(con, sql, args).then((r) => {
    if (!r) {
      return null
    } else {
      const keys = Object.keys(r)
      return (r as any)[keys[0]]
    }
  })
}
export function countTx(con: Connection, sql: string, args?: any[]): Promise<number> {
  return executeScalarTx<number>(con, sql, args).then((res) => (res !== null ? res : 0))
}

export function execute(con: Connection, sql: string, args?: any[]): Promise<number> {
  const p = toArray(args)
  return con.execute(sql, p).then((results) => results.rowsAffected ?? 0).finally(() => con.close())
}
export function query<T>(con: Connection, sql: string, args?: any[], m?: StringMap, bools?: Attribute[]): Promise<T[]> {
  const p = toArray(args)
  return con.execute<T>(sql, p).then((results) => {
    if (results.rows) {
      const x = results.metaData
      if (!x) {
        return results.rows
      } else {
        const arrayResult = results.rows.map((item) => {
          return formatData<T>(x, item)
        })
        return handleResults(arrayResult, m, bools)
      }
    } else {
      return []
    }
  }).finally(() => con.close())
}
export function queryOne<T>(con: Connection, sql: string, args?: any[], m?: StringMap, bools?: Attribute[]): Promise<T | null> {
  return query<T>(con, sql, args, m, bools).then((r) => {
    return r && r.length > 0 ? r[0] : null
  })
}
export function executeScalar<T>(con: Connection, sql: string, args?: any[]): Promise<T | null> {
  return queryOne<T>(con, sql, args).then((r) => {
    if (!r) {
      return null
    } else {
      const keys = Object.keys(r)
      return (r as any)[keys[0]]
    }
  })
}
export function count(con: Connection, sql: string, args?: any[]): Promise<number> {
  return executeScalar<number>(con, sql, args).then((res) => (res !== null ? res : 0))
}

export function insertBatch<T>(con: Connection | ((sql: string, args?: any[]) => Promise<number>), objs: T[], table: string, attrs: Attributes, ver?: string, notSkipInvalid?: boolean, buildParam?: (i: number) => string): Promise<number> {
  const s = buildToInsertBatch<T>(objs, table, attrs, ver, notSkipInvalid, buildParam)
  if (!s) {
    return Promise.resolve(-1)
  }
  if (typeof con === "function") {
    return con(s.query, s.params)
  } else {
    return execute(con, s.query, s.params)
  }
}
export function save<T>(con: Connection | ((sql: string, args?: any[]) => Promise<number>), obj: T, table: string, attrs: Attributes, ver?: string, buildParam?: (i: number) => string, i?: number): Promise<number> {
  const s = buildToSave(obj, table, attrs, ver, buildParam)
  if (!s) {
    return Promise.resolve(-1)
  }
  if (typeof con === "function") {
    return con(s.query, s.params)
  } else {
    return execute(con, s.query, s.params)
  }
}

export function saveBatch<T>(con: Connection | ((statements: Statement[]) => Promise<number>), objs: T[], table: string, attrs: Attributes, ver?: string, buildParam?: (i: number) => string): Promise<number> {
  const s = buildToSaveBatch(objs, table, attrs, ver, buildParam)
  if (typeof con === "function") {
    return con(s)
  } else {
    return executeBatch(con, s)
  }
}

export function toArray(arr?: any[]): any[] {
  if (!arr || arr.length === 0) {
    return []
  }
  const p: any[] = []
  const l = arr.length
  for (let i = 0; i < l; i++) {
    if (arr[i] === undefined || arr[i] == null) {
      p.push(null)
    } else {
      if (typeof arr[i] === "object") {
        if (arr[i] instanceof Date) {
          p.push(arr[i])
        } else {
          if (resource.string) {
            const s: string = JSON.stringify(arr[i])
            p.push(s)
          } else {
            p.push(arr[i])
          }
        }
      } else {
        p.push(arr[i])
      }
    }
  }
  return p
}
export function handleResults<T>(r: T[], m?: StringMap, bools?: Attribute[]): T[] {
  if (m) {
    const res = mapArray(r, m)
    if (bools && bools.length > 0) {
      return handleBool(res, bools)
    } else {
      return res
    }
  } else {
    if (bools && bools.length > 0) {
      return handleBool(r, bools)
    } else {
      return r
    }
  }
}
export function handleBool<T>(objs: T[], bools: Attribute[]) {
  if (!bools || bools.length === 0 || !objs) {
    return objs
  }
  for (const obj of objs) {
    const o: any = obj
    for (const field of bools) {
      if (field.name) {
        const v = o[field.name]
        if (typeof v !== "boolean" && v != null && v !== undefined) {
          const b = field.true
          if (b == null) {
            // tslint:disable-next-line:triple-equals
            o[field.name] = "1" == v || "T" == v || "Y" == v || "true" == v
          } else {
            // tslint:disable-next-line:triple-equals
            o[field.name] = v == b ? true : false
          }
        }
      }
    }
  }
  return objs
}
export function map<T>(obj: T, m?: StringMap): any {
  if (!m) {
    return obj
  }
  const mkeys = Object.keys(m)
  if (mkeys.length === 0) {
    return obj
  }
  const obj2: any = {}
  const keys = Object.keys(obj as any)
  for (const key of keys) {
    let k0 = m[key]
    if (!k0) {
      k0 = key
    }
    obj2[k0] = (obj as any)[key]
  }
  return obj2
}
export function mapArray<T>(results: T[], m?: StringMap): T[] {
  if (!m) {
    return results
  }
  const mkeys = Object.keys(m)
  if (mkeys.length === 0) {
    return results
  }
  const objs = []
  const length = results.length
  for (let i = 0; i < length; i++) {
    const obj = results[i]
    const obj2: any = {}
    const keys = Object.keys(obj as any)
    for (const key of keys) {
      let k0 = m[key]
      if (!k0) {
        k0 = key
      }
      obj2[k0] = (obj as any)[key]
    }
    objs.push(obj2)
  }
  return objs
}
export function getFields(fields: string[], all?: string[]): string[] | undefined {
  if (!fields || fields.length === 0) {
    return undefined
  }
  const ext: string[] = []
  if (all) {
    for (const s of fields) {
      if (all.includes(s)) {
        ext.push(s)
      }
    }
    if (ext.length === 0) {
      return undefined
    } else {
      return ext
    }
  } else {
    return fields
  }
}
export function buildFields(fields: string[], all?: string[]): string {
  const s = getFields(fields, all)
  if (!s || s.length === 0) {
    return "*"
  } else {
    return s.join(",")
  }
}
export function getMapField(name: string, mp?: StringMap): string {
  if (!mp) {
    return name
  }
  const x = mp[name]
  if (!x) {
    return name
  }
  if (typeof x === "string") {
    return x
  }
  return name
}
export function isEmpty(s: string): boolean {
  return !(s && s.length > 0)
}

// format the return data
// tslint:disable-next-line:array-type
export function formatData<T>(nameColumn: Metadata<T>[], data: any, m?: StringMap): T {
  const result: any = {}
  nameColumn.forEach((item, index) => {
    const key = m?.[item.name] ?? item.name
    result[key] = data[index]
  })
  return result
}

export function version(attrs: Attributes): Attribute | undefined {
  const ks = Object.keys(attrs)
  for (const k of ks) {
    const attr = attrs[k]
    if (attr.version) {
      attr.name = k
      return attr
    }
  }
  return undefined
}
// tslint:disable-next-line:max-classes-per-file
export class OracleBatchInserter<T> {
  protected version?: string
  protected param?: (i: number) => string
  constructor(
    protected connection: Connection,
    protected table: string,
    protected attributes: Attributes,
    protected map?: (v: T) => T,
    protected notSkipInvalid?: boolean,
    buildParam?: (i: number) => string,
  ) {
    this.write = this.write.bind(this)
    this.param = buildParam ? buildParam : param
    const x = version(attributes)
    if (x) {
      this.version = x.name
    }
  }
  write(objs: T[]): Promise<number> {
    if (!objs || objs.length === 0) {
      return Promise.resolve(0)
    }
    let list = objs
    if (this.map) {
      list = []
      for (const obj of objs) {
        const obj2 = this.map(obj)
        list.push(obj2)
      }
    }
    const stmt = buildToInsertBatch(list, this.table, this.attributes, this.version, this.notSkipInvalid, this.param)
    if (stmt) {
      return execute(this.connection, stmt.query, stmt.params)
    } else {
      return Promise.resolve(0)
    }
  }
}
// tslint:disable-next-line:max-classes-per-file
export class OracleWriter<T> {
  protected version?: string
  protected param?: (i: number) => string
  constructor(
    protected connection: Connection,
    protected table: string,
    protected attributes: Attributes,
    protected oneIfSuccess?: boolean,
    protected map?: (v: T) => T,
    buildParam?: (i: number) => string,
  ) {
    this.write = this.write.bind(this)
    this.param = buildParam ? buildParam : param
    const x = version(attributes)
    if (x) {
      this.version = x.name
    }
  }
  write(obj: T): Promise<number> {
    if (!obj) {
      return Promise.resolve(0)
    }
    let obj2: NonNullable<T> | T = obj
    if (this.map) {
      obj2 = this.map(obj)
    }
    const stmt = buildToSave(obj2, this.table, this.attributes, this.version, this.param)
    if (stmt) {
      if (this.oneIfSuccess) {
        return execute(this.connection, stmt.query, stmt.params).then((ct) => (ct > 0 ? 1 : 0))
      } else {
        return execute(this.connection, stmt.query, stmt.params)
      }
    } else {
      return Promise.resolve(0)
    }
  }
}
// tslint:disable-next-line:max-classes-per-file
export class OracleStreamWriter<T> {
  list: T[] = []
  version?: string
  protected param?: (i: number) => string
  constructor(
    protected connection: Connection | ((statements: Statement[]) => Promise<number>),
    protected table: string,
    protected attributes: Attributes,
    protected size: number = 5000,
    protected map?: (v: T) => T,
    buildParam?: (i: number) => string,
  ) {
    this.write = this.write.bind(this)
    this.flush = this.flush.bind(this)
    this.param = buildParam
    const x = version(attributes)
    if (x) {
      this.version = x.name
    }
  }
  write(obj: T): Promise<number> {
    if (!obj) {
      return Promise.resolve(0)
    }
    let obj2: NonNullable<T> | T = obj
    if (this.map) {
      obj2 = this.map(obj)
      this.list.push(obj2)
    } else {
      this.list.push(obj)
    }
    if (this.list.length < this.size) {
      return Promise.resolve(0)
    } else {
      return this.flush()
    }
  }
  flush(): Promise<number> {
    if (!this.list || this.list.length === 0) {
      return Promise.resolve(0)
    } else {
      const total = this.list.length
      const stmt = buildToSaveBatch(this.list, this.table, this.attributes, this.version, this.param)
      if (stmt) {
        return executeBatch(this.connection as any, stmt).then((r) => {
          this.list = []
          return total
        })
      } else {
        return Promise.resolve(0)
      }
    }
  }
}
// tslint:disable-next-line:max-classes-per-file
export class OracleBatchWriter<T> {
  version?: string
  param?: (i: number) => string
  constructor(
    protected connection: Connection,
    protected table: string,
    protected attributes: Attributes,
    protected map?: (v: T) => T,
    buildParam?: (i: number) => string,
  ) {
    this.write = this.write.bind(this)
    this.param = buildParam ? buildParam : param
    const x = version(attributes)
    if (x) {
      this.version = x.name
    }
  }
  write(objs: T[]): Promise<number> {
    if (!objs || objs.length === 0) {
      return Promise.resolve(0)
    }
    let list = objs
    if (this.map) {
      list = []
      for (const obj of objs) {
        const obj2 = this.map(obj)
        list.push(obj2)
      }
    }
    const stmts = buildToSaveBatch(list, this.table, this.attributes, this.version, this.param)
    if (stmts && stmts.length > 0) {
      return executeBatch(this.connection, stmts)
    } else {
      return Promise.resolve(0)
    }
  }
}
// tslint:disable-next-line:max-classes-per-file
export class Exporter<T> {
  constructor(
    protected connection: Connection,
    protected filename: string,
    protected attributes: Attributes,
    protected buildQuery: (ctx?: any) => Promise<Statement>,
    protected format: (row: T) => string,
    protected write: (chunk: string) => boolean,
    protected end: (cb?: () => void) => void,
    protected logInfo?: (msg: string, m?: SimpleMap) => void,
    protected progressSize: number = 10000,
    protected isClose: boolean = true,
  ) {
    this.export = this.export.bind(this)
  }
  async export(ctx?: any): Promise<number> {
    const stmt = await this.buildQuery(ctx)
    const stream = this.connection.queryStream(stmt.query, stmt.params || {})

    return new Promise<number>((resolve, reject) => {
      let metaData: [{ name: string }]
      let i = 0
      let j = 0
      let errorHandled = false

      stream.on("metadata", (metadata: any) => (metaData = metadata))

      stream.on("data", (row: any[]) => {
        i++
        j++
        const obj = convertToObject(row, metaData, this.attributes)
        const exportStr = this.format(obj as any)
        this.write(exportStr)
        if (j >= this.progressSize) {
          if (this.logInfo) {
            this.logInfo(`Progress: ${i} records processed of file '${this.filename}'`)
          }
          j = 0
        }
      })
      stream.on("error", async (error: any) => {
        if (errorHandled) {
          return
        }
        errorHandled = true
        try {
          if (this.isClose) {
            await closeConnection(this.connection)
          }
        } finally {
          reject(error)
        }
      })
      stream.on("end", async () => {
        if (errorHandled) {
          return
        }
        try {
          stream.destroy()

          this.end()
          if (this.isClose) {
            await closeConnection(this.connection)
          }
          resolve(i)
        } catch (error) {
          reject(error)
        }
      })
    })
  }
}
export interface SimpleMap {
  [key: string]: string | number | boolean | Date
}
export interface FileWriter {
  write(chunk: string): boolean
  flush?(cb?: () => void): void
  end?(cb?: () => void): void
}
export interface Formatter<T> {
  format: (row: T) => string
}
export interface QueryBuilder {
  build(ctx?: any): Promise<Statement>
}
// tslint:disable-next-line:max-classes-per-file
export class ExportService<T> {
  constructor(
    protected connection: Connection,
    protected filename: string,
    protected attributes: Attributes,
    protected queryBuilder: QueryBuilder,
    protected formatter: Formatter<T>,
    protected writer: FileWriter,
    protected logInfo?: (msg: string, m?: SimpleMap) => void,
    protected progressSize: number = 10000,
    protected isClose: boolean = true,
  ) {
    this.export = this.export.bind(this)
  }
  async export(ctx?: any): Promise<number> {
    const stmt = await this.queryBuilder.build(ctx)
    const stream = this.connection.queryStream(stmt.query, stmt.params || {})
    return new Promise<number>((resolve, reject) => {
      let metaData: [{ name: string }]
      let i = 0
      let j = 0
      let errorHandled = false
      stream.on("metadata", (metadata: any) => (metaData = metadata))
      stream.on("data", (row: any[]) => {
        i++
        j++
        const obj = convertToObject(row, metaData, this.attributes)
        const exportStr = this.formatter.format(obj as any)
        this.writer.write(exportStr)
        if (j >= this.progressSize) {
          if (this.logInfo) {
            this.logInfo(`Progress: ${i} records processed of file '${this.filename}'`)
          }
          j = 0
        }
      })

      stream.on("error", async (error: any) => {
        if (errorHandled) {
          return
        }
        errorHandled = true
        try {
          if (this.isClose) {
            await closeConnection(this.connection)
          }
        } finally {
          reject(error)
        }
      })
      stream.on("end", async () => {
        if (errorHandled) {
          return
        }
        try {
          stream.destroy()

          if (this.writer.flush) {
            this.writer.flush()
          }

          if (this.writer.end) {
            this.writer.end()
          }

          if (this.isClose) {
            await closeConnection(this.connection)
          }
          resolve(i)
        } catch (error) {
          reject(error)
        }
      })
    })
  }
}

async function closeConnection(connection: Connection) {
  if (!connection) {
    return
  }
  try {
    await connection.close()
  } catch (err) {
    console.error(err)
  }
}
function convertToObject(row: any[], metadata: [{ name: string }], attributes: Attributes): any {
  const rsl: { [key: string]: any } = {}
  for (const [key, value] of Object.entries(row)) {
    const keyAsInt = parseInt(key, 10)

    if (keyAsInt >= metadata.length) {
      console.warn(`The provided metadata does not match`)
      break
    }

    let isFound = false
    const propName = metadata[keyAsInt].name.toLowerCase()

    for (const [attrKey, attrVal] of Object.entries(attributes)) {
      if (attrVal.column === propName || attrKey.toLowerCase() === propName) {
        rsl[attrKey] = value
        isFound = true
        break
      }
    }
    if (!isFound) {
      console.warn(`The property "${propName}" is not found`)
    }
  }
  return rsl
}
export function select(table: string, attrs: Attributes): string {
  const cols: string[] = []
  const ks = Object.keys(attrs)
  for (const k of ks) {
    const attr = attrs[k]
    attr.name = k
    const field = attr.column ? attr.column : k
    cols.push(field)
  }
  return `select ${cols.join(",")} from ${table}`
}
