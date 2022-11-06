import { Connection, getConnection as getConnection2, Metadata } from 'oracledb';
import { buildToInsertBatch, buildToSave, buildToSaveBatch } from './build';
import { Attribute, Attributes, Manager, Statement, StringMap } from './metadata';

export * from './metadata';
export * from './build';
export * from './checker';

// OracleDB.autoCommit = true;

// tslint:disable-next-line:class-name
export class resource {
  static string?: boolean;
}

export interface ServiceConfig {
  host: string;
  user: string;
  password: string;
  port: number;
  service_name: string;
}
export function getConnection(conf: ServiceConfig): Promise<Connection> {
  return getConnection2({
    user: conf.user,
    password: conf.password,
    connectionString: `(
      DESCRIPTION =
        (ADDRESS = (PROTOCOL=TCP) (Host=${conf.host}) (Port=${conf.port}))
        (CONNECT_DATA = (SERVICE_NAME=${conf.service_name}))
    )`
  });
}
export class OracleManager implements Manager {
  constructor(public conn: Connection) {
    this.param = this.param.bind(this);
    this.exec = this.exec.bind(this);
    this.execBatch = this.execBatch.bind(this);
    this.query = this.query.bind(this);
    this.queryOne = this.queryOne.bind(this);
    this.execScalar = this.execScalar.bind(this);
    this.count = this.count.bind(this);
  }
  driver = 'oracle';
  param(i: number): string {
    return ':' + i;
  }
  exec(sql: string, args?: any[], ctx?: any): Promise<number> {
    const p = (ctx ? ctx : this.conn);
    return exec(p, sql, args);
  }
  execBatch(statements: Statement[], firstSuccess?: boolean, ctx?: any): Promise<number> {
    const p = (ctx ? ctx : this.conn);
    return execBatch(p, statements, firstSuccess);
  }
  query<T>(sql: string, args?: any[], m?: StringMap, bools?: Attribute[], ctx?: any): Promise<T[]> {
    const p = (ctx ? ctx : this.conn);
    return query(p, sql, args, m, bools);
  }
  queryOne<T>(sql: string, args?: any[], m?: StringMap, bools?: Attribute[], ctx?: any): Promise<T|null> {
    const p = (ctx ? ctx : this.conn);
    return queryOne(p, sql, args, m, bools);
  }
  execScalar<T>(sql: string, args?: any[], ctx?: any): Promise<T> {
    const p = (ctx ? ctx : this.conn);
    return execScalar<T>(p, sql, args);
  }
  count(sql: string, args?: any[], ctx?: any): Promise<number> {
    const p = (ctx ? ctx : this.conn);
    return count(p, sql, args);
  }
}

export async function execBatch(conn: Connection, statements: Statement[], firstSuccess?: boolean): Promise<number> {
  if (!statements || statements.length === 0) {
    return Promise.resolve(0);
  } else if (statements.length === 1) {
    return exec(conn, statements[0].query, statements[0].params);
  }
  let c = 0;
  try {
    if (firstSuccess) {
      const result0 = await conn.execute(statements[0].query, statements[0].params as any, { autoCommit: false });
      if (result0 && result0.rowsAffected && result0.rowsAffected > 0) {
        const subs = statements.slice(1);
        const arrPromise = subs.map((item) => {
          return conn.execute(item.query, item.params ? item.params : [], { autoCommit: false });
        });
        const results = await Promise.all(arrPromise);
        for (const obj of results) {
          if (obj.rowsAffected) {
            c += obj.rowsAffected;
          }
        }
        if (result0.rowsAffected) {
          c += result0.rowsAffected;
        }
        await conn.commit();
        return c;
      } else {
        await conn.commit();
        return c;
      }
    } else {
      const arrPromise = statements.map((item) => conn.execute(item.query, item.params ? item.params : [], { autoCommit: false }));
      const results = await Promise.all(arrPromise);
      for (const obj of results) {
        if (obj.rowsAffected) {
          c += obj.rowsAffected;
        }
      }
      await conn.commit();
      return c;
    }
  } catch (e) {
    await conn.rollback();
    // console.log(e);
    throw e;
  }
  finally {
    conn.release();
  }
}

export function exec(conn: Connection, sql: string, args?: any[]): Promise<number> {
  const p = toArray(args);
  return new Promise<number>((resolve, reject) => {
    return conn.execute(sql, p, (err, results) => {
      if (err) {
        // console.log(err);
        return reject(err);
      } else {
        if (results.rowsAffected) {
          return resolve(results.rowsAffected);
        } else {
          return resolve(-1);
        }
      }
    });
  });
}

export function query<T>(conn: Connection, sql: string, args?: any[], m?: StringMap, bools?: Attribute[]): Promise<T[]> {
  const p = toArray(args);
  return new Promise<T[]>((resolve, reject) => {
    return conn.execute<T>(sql, p, (err, results) => {
      if (err) {
        return reject(err);
      } else {
        if (results.rows) {
          const x = results.metaData;
          if (!x) {
            return resolve(results.rows);
          } else {
            const arrayResult = results.rows.map(item => {
              return formatData<T>(x, item);
            });
            return resolve(handleResults(arrayResult, m, bools));
          }
        } else {
          return resolve([]);
        }
      }
    });
  });
}

export function queryOne<T>(conn: Connection, sql: string, args?: any[], m?: StringMap, bools?: Attribute[]): Promise<T|null> {
  return query<T>(conn, sql, args, m, bools).then(r => {
    return (r && r.length > 0 ? r[0] : null);
  });
}

export function execScalar<T>(conn: Connection, sql: string, args?: any[]): Promise<T> {
  return queryOne<T>(conn, sql, args).then(r => {
    if (!r) {
      return null;
    } else {
      const keys = Object.keys(r);
      return (r as any)[keys[0]];
    }
  });
}

export function count(conn: Connection, sql: string, args?: any[]): Promise<number> {
  return execScalar<number>(conn, sql, args);
}
export function insertBatch<T>(conn: Connection | ((sql: string, args?: any[]) => Promise<number>), objs: T[], table: string, attrs: Attributes, ver?: string, notSkipInvalid?: boolean, buildParam?: (i: number) => string): Promise<number> {
  const s = buildToInsertBatch<T>(objs, table, attrs, ver, notSkipInvalid, buildParam);
  if (!s) {
    return Promise.resolve(-1);
  }
  if (typeof conn === 'function') {
    return conn(s.query, s.params);
  } else {
    return exec(conn, s.query, s.params);
  }
}
export function save<T>(conn: Connection | ((sql: string, args?: any[]) => Promise<number>), obj: T, table: string, attrs: Attributes, ver?: string, buildParam?: (i: number) => string, i?: number): Promise<number> {
  const s = buildToSave(obj, table, attrs, ver, buildParam);
  if (!s) {
    return Promise.resolve(-1);
  }
  if (typeof conn === 'function') {
    return conn(s.query, s.params);
  } else {
    return exec(conn, s.query, s.params);
  }
}

export function saveBatch<T>(conn: Connection | ((statements: Statement[]) => Promise<number>), objs: T[], table: string, attrs: Attributes, ver?: string, buildParam?: (i: number) => string): Promise<number> {
  const s = buildToSaveBatch(objs, table, attrs, ver, buildParam);
  if (typeof conn === 'function') {
    return conn(s);
  } else {
    return execBatch(conn, s);
  }
}

export function toArray(arr?: any[]): any[] {
  if (!arr || arr.length === 0) {
    return [];
  }
  const p: any[] = [];
  const l = arr.length;
  for (let i = 0; i < l; i++) {
    if (arr[i] === undefined || arr[i] == null) {
      p.push(null);
    } else {
      if (typeof arr[i] === 'object') {
        if (arr[i] instanceof Date) {
          p.push(arr[i]);
        } else {
          if (resource.string) {
            const s: string = JSON.stringify(arr[i]);
            p.push(s);
          } else {
            p.push(arr[i]);
          }
        }
      } else {
        p.push(arr[i]);
      }
    }
  }
  return p;
}
export function handleResults<T>(r: T[], m?: StringMap, bools?: Attribute[]): T[] {
  if (m) {
    const res = mapArray(r, m);
    if (bools && bools.length > 0) {
      return handleBool(res, bools);
    } else {
      return res;
    }
  } else {
    if (bools && bools.length > 0) {
      return handleBool(r, bools);
    } else {
      return r;
    }
  }
}
export function handleBool<T>(objs: T[], bools: Attribute[]) {
  if (!bools || bools.length === 0 || !objs) {
    return objs;
  }
  for (const obj of objs) {
    const o: any = obj;
    for (const field of bools) {
      if (field.name) {
        const v = o[field.name];
        if (typeof v !== 'boolean' && v != null && v !== undefined) {
          const b = field.true;
          if (b == null || b === undefined) {
            // tslint:disable-next-line:triple-equals
            o[field.name] = ('1' == v || 'T' == v || 'Y' == v || 'true' == v);
          } else {
            // tslint:disable-next-line:triple-equals
            o[field.name] = (v == b ? true : false);
          }
        }
      }
    }
  }
  return objs;
}
export function map<T>(obj: T, m?: StringMap): any {
  if (!m) {
    return obj;
  }
  const mkeys = Object.keys(m);
  if (mkeys.length === 0) {
    return obj;
  }
  const obj2: any = {};
  const keys = Object.keys(obj as any);
  for (const key of keys) {
    let k0 = m[key];
    if (!k0) {
      k0 = key;
    }
    obj2[k0] = (obj as any)[key];
  }
  return obj2;
}
export function mapArray<T>(results: T[], m?: StringMap): T[] {
  if (!m) {
    return results;
  }
  const mkeys = Object.keys(m);
  if (mkeys.length === 0) {
    return results;
  }
  const objs = [];
  const length = results.length;
  for (let i = 0; i < length; i++) {
    const obj = results[i];
    const obj2: any = {};
    const keys = Object.keys(obj as any);
    for (const key of keys) {
      let k0 = m[key];
      if (!k0) {
        k0 = key;
      }
      obj2[k0] = (obj as any)[key];
    }
    objs.push(obj2);
  }
  return objs;
}
export function getFields(fields: string[], all?: string[]): string[]|undefined {
  if (!fields || fields.length === 0) {
    return undefined;
  }
  const ext: string [] = [];
  if (all) {
    for (const s of fields) {
      if (all.includes(s)) {
        ext.push(s);
      }
    }
    if (ext.length === 0) {
      return undefined;
    } else {
      return ext;
    }
  } else {
    return fields;
  }
}
export function buildFields(fields: string[], all?: string[]): string {
  const s = getFields(fields, all);
  if (!s || s.length === 0) {
    return '*';
  } else {
    return s.join(',');
  }
}
export function getMapField(name: string, mp?: StringMap): string {
  if (!mp) {
    return name;
  }
  const x = mp[name];
  if (!x) {
    return name;
  }
  if (typeof x === 'string') {
    return x;
  }
  return name;
}
export function isEmpty(s: string): boolean {
  return !(s && s.length > 0);
}

// format the return data
// tslint:disable-next-line:array-type
export function formatData<T>(nameColumn: Metadata<T>[], data: any, m?: StringMap): T {
  const result: any = {};
  nameColumn.forEach((item, index) => {
    let key = item.name;
    if (m) {
      key = m[item.name];
    }
    result[key] = data[index];
  });
  return result;
}

export function version(attrs: Attributes): Attribute|undefined {
  const ks = Object.keys(attrs);
  for (const k of ks) {
    const attr = attrs[k];
    if (attr.version) {
      attr.name = k;
      return attr;
    }
  }
  return undefined;
}
// tslint:disable-next-line:max-classes-per-file
export class OracleBatchInserter<T> {
  connection?: Connection;
  version?: string;
  exec?: (sql: string, args?: any[]) => Promise<number>;
  map?: (v: T) => T;
  param?: (i: number) => string;
  constructor(conn: Connection | ((sql: string, args?: any[]) => Promise<number>), public table: string, public attributes: Attributes, toDB?: (v: T) => T, public notSkipInvalid?: boolean, buildParam?: (i: number) => string) {
    this.write = this.write.bind(this);
    if (typeof conn === 'function') {
      this.exec = conn;
    } else {
      this.connection = conn;
    }
    this.param = buildParam;
    this.map = toDB;
    const x = version(attributes);
    if (x) {
      this.version = x.name;
    }
  }
  write(objs: T[]): Promise<number> {
    if (!objs || objs.length === 0) {
      return Promise.resolve(0);
    }
    let list = objs;
    if (this.map) {
      list = [];
      for (const obj of objs) {
        const obj2 = this.map(obj);
        list.push(obj2);
      }
    }
    const stmt = buildToInsertBatch(list, this.table, this.attributes, this.version, this.notSkipInvalid, this.param);
    if (stmt) {
      if (this.exec) {
        return this.exec(stmt.query, stmt.params);
      } else {
        return exec(this.connection as any, stmt.query, stmt.params);
      }
    } else {
      return Promise.resolve(0);
    }
  }
}
// tslint:disable-next-line:max-classes-per-file
export class OracleWriter<T> {
  connection?: Connection;
  version?: string;
  exec?: (sql: string, args?: any[]) => Promise<number>;
  map?: (v: T) => T;
  param?: (i: number) => string;
  constructor(conn: Connection | ((sql: string, args?: any[]) => Promise<number>), public table: string, public attributes: Attributes, public oneIfSuccess?: boolean, toDB?: (v: T) => T, buildParam?: (i: number) => string) {
    this.write = this.write.bind(this);
    if (typeof conn === 'function') {
      this.exec = conn;
    } else {
      this.connection = conn;
    }
    this.param = buildParam;
    this.map = toDB;
    const x = version(attributes);
    if (x) {
      this.version = x.name;
    }
  }
  write(obj: T): Promise<number> {
    if (!obj) {
      return Promise.resolve(0);
    }
    let obj2: NonNullable<T> | T = obj;
    if (this.map) {
      obj2 = this.map(obj);
    }
    const stmt = buildToSave(obj2, this.table, this.attributes, this.version, this.param);
    if (stmt) {
      if (this.exec) {
        if (this.oneIfSuccess) {
          return this.exec(stmt.query, stmt.params).then(ct => ct > 0 ? 1 : 0);
        } else {
          return this.exec(stmt.query, stmt.params);
        }
      } else {
        if (this.oneIfSuccess) {
          return exec(this.connection as any, stmt.query, stmt.params).then(ct => ct > 0 ? 1 : 0);
        } else {
          return exec(this.connection as any, stmt.query, stmt.params);
        }
      }
    } else {
      return Promise.resolve(0);
    }
  }
}
// tslint:disable-next-line:max-classes-per-file
export class OracleStreamWriter<T> {
  list: T[] = [];
  size = 0;
  connection?: Connection;
  version?: string;
  execBatch?: (statements: Statement[]) => Promise<number>;
  map?: (v: T) => T;
  param?: (i: number) => string;
  constructor(con: Connection | ((statements: Statement[]) => Promise<number>), public table: string, public attributes: Attributes, size?: number, toDB?: (v: T) => T, buildParam?: (i: number) => string) {
    this.write = this.write.bind(this);
    this.flush = this.flush.bind(this);
    if (typeof con === 'function') {
      this.execBatch = con;
    } else {
      this.connection = con;
    }
    this.param = buildParam;
    this.map = toDB;
    const x = version(attributes);
    if (x) {
      this.version = x.name;
    }
    if (size) {
      this.size = size;
    }
  }
  write(obj: T): Promise<number> {
    if (!obj) {
      return Promise.resolve(0);
    }
    let obj2: NonNullable<T> | T = obj;
    if (this.map) {
      obj2 = this.map(obj);
      this.list.push(obj2);
    } else {
      this.list.push(obj);
    }
    if (this.list.length < this.size) {
      return Promise.resolve(0);
    } else {
      return this.flush();
    }
  }
  flush(): Promise<number> {
    if (!this.list || this.list.length === 0) {
      return Promise.resolve(0);
    } else {
      const total = this.list.length;
      const stmt = buildToSaveBatch(this.list, this.table, this.attributes, this.version, this.param);
      if (stmt) {
        if (this.execBatch) {
          return this.execBatch(stmt).then(r => {
            this.list = [];
            return total;
          });
        } else {
          return execBatch(this.connection as any, stmt).then(r => {
            this.list = [];
            return total;
          });
        }
      } else {
        return Promise.resolve(0);
      }
    }
  }
}
// tslint:disable-next-line:max-classes-per-file
export class OracleBatchWriter<T> {
  connection?: Connection;
  version?: string;
  execute?: (statements: Statement[]) => Promise<number>;
  map?: (v: T) => T;
  param?: (i: number) => string;
  constructor(conn: Connection | ((statements: Statement[]) => Promise<number>), public table: string, public attributes: Attributes, toDB?: (v: T) => T, buildParam?: (i: number) => string) {
    this.write = this.write.bind(this);
    if (typeof conn === 'function') {
      this.execute = conn;
    } else {
      this.connection = conn;
    }
    this.param = buildParam;
    this.map = toDB;
    const x = version(attributes);
    if (x) {
      this.version = x.name;
    }
  }
  write(objs: T[]): Promise<number> {
    if (!objs || objs.length === 0) {
      return Promise.resolve(0);
    }
    let list = objs;
    if (this.map) {
      list = [];
      for (const obj of objs) {
        const obj2 = this.map(obj);
        list.push(obj2);
      }
    }
    const stmts = buildToSaveBatch(list, this.table, this.attributes, this.version, this.param);
    if (stmts && stmts.length > 0) {
      if (this.execute) {
        return this.execute(stmts);
      } else {
        return execBatch(this.connection as any, stmts);
      }
    } else {
      return Promise.resolve(0);
    }
  }
}
// tslint:disable-next-line:max-classes-per-file
export class Exporter<T> {
  constructor(
    public connection: Connection,
    public attributes: Attributes,
    public buildQuery: (ctx?: any) => Promise<Statement>,
    public format: (row: T) => string,
    public write: (chunk: string) => boolean,
    public end: (cb?: () => void) => void) {
  }
  async export(ctx?: any): Promise<number> {
    const idx = -1;
    const stmt = await this.buildQuery(ctx);
    const stream = this.connection.queryStream(stmt.query, stmt.params || {});
    let metaData: [{name: string}];
    // access metadata of query (IF NEED)
    stream.on('metadata', (metadata: any) => metaData = metadata);
    // handle data row...
    stream.on('data', (row: any[]) => {
      const obj = convertToObject(row, metaData, this.attributes);
      // this.write(this.format(obj as any))
      const exportStr = this.format(obj as any);
      this.write(exportStr);
    });
    // handle any error if occurred
    stream.on('error', async (error: any) => {
      console.error(error);
      await closeConnection(this.connection);
    });
    // all data has been fetched ...
    // the stream should be closed when it has been finished
    stream.on('end', () => {
      stream.destroy();
      this.end();
    });
    // can now close connection...  (Note: do not close connections on 'end')
    stream.on('close', async () => await closeConnection(this.connection));
    return idx;
  }
}
export interface FileWriter {
  write(chunk: string): boolean;
  flush?(cb?: () => void): void;
  end?(cb?: () => void): void;
}
export interface Formatter<T> {
  format: (row: T) => string;
}
export interface QueryBuilder {
  build(cxt?: any): Promise<Statement>;
}
// tslint:disable-next-line:max-classes-per-file
export class ExportService<T> {
  constructor(
    public connection: Connection,
    public attributes: Attributes,
    public queryBuilder: QueryBuilder,
    public formatter: Formatter<T>,
    public writer: FileWriter) {
  }
  async export(ctx?: any): Promise<number> {
    const idx = -1;
    const stmt = await this.queryBuilder.build(ctx);
    const stream = this.connection.queryStream(stmt.query, stmt.params || {});
    let metaData: [{name: string}];
    // access metadata of query (IF NEED)
    stream.on('metadata', (metadata: any) => metaData = metadata);
    // handle data row...
    stream.on('data', (row: any[]) => {
      const obj = convertToObject(row, metaData, this.attributes);
      // this.write(this.format(obj as any))
      const exportStr = this.formatter.format(obj as any);
      this.writer.write(exportStr);
    });
    // handle any error if occurred
    stream.on('error', async (error: any) => {
      console.error(error);
      await closeConnection(this.connection);
    });
    // all data has been fetched ...
    // the stream should be closed when it has been finished
    stream.on('end', () => {
      stream.destroy();

      if (this.writer.end) {
        this.writer.end();
      } else if (this.writer.flush) {
        this.writer.flush();
      }
    });
    // can now close connection...  (Note: do not close connections on 'end')
    stream.on('close', async () => await closeConnection(this.connection));
    return idx;
  }
}
async function closeConnection(connection: Connection) {
  if (!connection) {
    return;
  }
  try {
    await connection.close();
  } catch (err) {
    console.error(err);
  }
}
function convertToObject(row: any[], metadata: [{name: string}], attributes: Attributes): any {
  const rsl: {[key: string]: any} = {};
  for (const [key, value] of Object.entries(row)) {
    const keyAsInt = parseInt(key, 10);

    if (keyAsInt >= metadata.length) {
      console.warn(`The provided metadata does not match`);
      break;
    }

    let isFound = false;
    const propName = metadata[keyAsInt].name.toLowerCase();

    for (const [attrKey, attrVal] of Object.entries(attributes)) {
      if (attrVal.column === propName || attrKey.toLowerCase() === propName) {
        rsl[attrKey] = value;
        isFound = true;
        break;
      }
    }
    if (!isFound) {
      console.warn(`The property "${propName}" is not found`);
    }
  }
  return rsl;
}
