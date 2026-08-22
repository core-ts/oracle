var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
  function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
  return new (P || (P = Promise))(function (resolve, reject) {
    function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
    function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
    function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
    step((generator = generator.apply(thisArg, _arguments || [])).next());
  });
};
export function param(i) {
  return ":" + i;
}
export function params(length, from) {
  if (from == null) {
    from = 0;
  }
  const ps = [];
  for (let i = 1; i <= length; i++) {
    ps.push(param(i + from));
  }
  return ps;
}
export function metadata(attrs) {
  const mp = {};
  const ks = Object.keys(attrs);
  const ats = [];
  const bools = [];
  const fields = [];
  const m = { keys: ats, fields };
  let isMap = false;
  for (const k of ks) {
    const attr = attrs[k];
    attr.name = k;
    if (attr.key) {
      ats.push(attr);
    }
    if (!attr.ignored) {
      fields.push(k);
    }
    if (attr.type === "boolean") {
      bools.push(attr);
    }
    if (attr.version) {
      m.version = k;
    }
    const field = attr.column ? attr.column : k;
    const s = field.toLowerCase();
    if (s !== k) {
      mp[s] = k;
      isMap = true;
    }
  }
  if (isMap) {
    m.map = mp;
  }
  if (bools.length > 0) {
    m.bools = bools;
  }
  return m;
}
export function buildToInsertBatch(objs, table, attrs, ver, notSkipInvalid, buildParam) {
  if (!buildParam) {
    buildParam = param;
  }
  let i = 1;
  const ks = Object.keys(attrs);
  const args = [];
  const rows = [];
  for (const obj of objs) {
    const cols = [];
    const values = [];
    let isVersion = false;
    for (const k of ks) {
      let v = obj[k];
      const attr = attrs[k];
      if (attr && !attr.ignored && !attr.noinsert) {
        if (v == null) {
          v = attr.default;
        }
        if (v != null) {
          const field = attr.column ? attr.column : k;
          cols.push(field);
          if (k === ver) {
            isVersion = true;
            values.push(`${1}`);
          }
          else {
            if (v === "") {
              values.push(`''`);
            }
            else if (typeof v === "number") {
              values.push(toString(v));
            }
            else if (typeof v === "boolean") {
              values.push(buildParam(i++));
              if (v === true) {
                const v2 = attr.true !== undefined ? attr.true : `1`;
                args.push(v2);
              }
              else {
                const v2 = attr.false !== undefined ? attr.false : `0`;
                args.push(v2);
              }
            }
            else {
              const p = buildParam(i++);
              values.push(p);
              args.push(v);
            }
          }
        }
      }
    }
    if (!isVersion && ver && ver.length > 0) {
      const attr = attrs[ver];
      if (attr) {
        const field = attr.column ? attr.column : ver;
        cols.push(field);
        values.push(`${1}`);
      }
    }
    if (cols.length === 0) {
      if (notSkipInvalid) {
        return { query: "", params: args };
      }
    }
    else {
      const s = `into ${table}(${cols.join(",")})values(${values.join(",")})`;
      rows.push(s);
    }
  }
  if (rows.length === 0) {
    return { query: "", params: args };
  }
  const query = `insert all ${rows.join(" ")} select * from dual`;
  return { query, params: args };
}
export function buildToSave(obj, table, attrs, pks, ver, buildParam, i) {
  if (i == null) {
    i = 1;
  }
  if (!buildParam) {
    buildParam = param;
  }
  const cols = [];
  const values = [];
  const args = [];
  const ks = Object.keys(attrs);
  if (!pks) {
    pks = [];
    for (const k of ks) {
      const attr = attrs[k];
      attr.name = k;
      if (attr.key) {
        pks.push(attr);
      }
      if (attr.version) {
        ver = k;
      }
    }
  }
  const colQuery = [];
  const colSet = [];
  let isUpdate = true;
  for (const k of pks) {
    if (k.name) {
      let v = obj[k.name];
      if (v == null) {
        isUpdate = false;
      }
    }
  }
  if (pks.length > 0 && isUpdate) {
    for (const pk of pks) {
      if (pk.name) {
        const attr = attrs[pk.name];
        let v = obj[pk.name];
        if (v == null) {
          v = attr.default;
        }
        const field = attr.column ? attr.column : pk.name;
        let x;
        if (v === "") {
          x = `''`;
        }
        else if (typeof v === "number") {
          x = toString(v);
        }
        else {
          x = buildParam(i++);
          if (typeof v === "boolean") {
            if (v === true) {
              const v2 = attr.true !== undefined ? attr.true : `1`;
              args.push(v2);
            }
            else {
              const v2 = attr.false !== undefined ? attr.false : `0`;
              args.push(v2);
            }
          }
          else {
            args.push(v);
          }
        }
        colQuery.push(`${field}=${x}`);
      }
    }
    for (const k of ks) {
      const v = obj[k];
      if (v !== undefined) {
        const attr = attrs[k];
        if (attr && !attr.key && !attr.ignored && !attr.noupdate) {
          const field = attr.column ? attr.column : k;
          let x;
          if (attr.version) {
            ver = k;
            x = `${field} + 1`;
          }
          else {
            if (v === null) {
              x = "null";
            }
            else if (v === "") {
              x = `''`;
            }
            else if (typeof v === "number") {
              x = toString(v);
            }
            else {
              x = buildParam(i++);
              if (typeof v === "boolean") {
                if (v === true) {
                  const v2 = attr.true !== undefined ? attr.true : `1`;
                  args.push(v2);
                }
                else {
                  const v2 = attr.false !== undefined ? attr.false : `0`;
                  args.push(v2);
                }
              }
              else {
                args.push(v);
              }
            }
          }
          colSet.push(`${field}=${x}`);
        }
      }
    }
  }
  for (const k of ks) {
    const attr = attrs[k];
    if (!attr) {
      continue;
    }
    let v = obj[k];
    if (v == null) {
      v = attr.default;
    }
    if (v != null && !attr.ignored && !attr.noinsert) {
      const field = attr.column ? attr.column : k;
      cols.push(field);
      if (attr.version) {
        ver = k;
        values.push(`${1}`);
      }
      else {
        if (v === "") {
          values.push(`''`);
        }
        else if (typeof v === "number") {
          values.push(toString(v));
        }
        else {
          const p = buildParam(i++);
          values.push(p);
          if (typeof v === "boolean") {
            if (v === true) {
              const v2 = attr.true !== undefined ? attr.true : `1`;
              args.push(v2);
            }
            else {
              const v2 = attr.false !== undefined ? attr.false : `0`;
              args.push(v2);
            }
          }
          else {
            args.push(v);
          }
        }
      }
    }
  }
  if (isUpdate === false || pks.length === 0) {
    if (cols.length === 0) {
      return { query: "", params: args };
    }
    else {
      if (pks.length === 0) {
        const q = `insert into ${table}(${cols.join(",")})values(${values.join(",")})`;
        return { query: q, params: args };
      }
      else {
        const query = `merge into ${table} using dual on (${colQuery.join(" and ")})
  when not matched then insert (${cols.join(",")})
  values (${values.join(",")})`;
        return { query, params: args };
      }
    }
  }
  if (colSet.length > 0) {
    const query = `merge into ${table} using dual on (${colQuery.join(" and ")})
    when matched then update set ${colSet.join(",")}
    when not matched then insert (${cols.join(",")})
      values (${values.join(",")})`;
    return { query, params: args };
  }
  else {
    if (cols.length > 0) {
      const query = `merge into ${table} using dual on (${colQuery.join(" and ")})
  when not matched then insert (${cols.join(",")})
  values (${values.join(",")})`;
      return { query, params: args };
    }
    else {
      return { query: "", params: args };
    }
  }
}
export function buildToSaveBatch(objs, table, attrs, pks, ver, buildParam) {
  if (!buildParam) {
    buildParam = param;
  }
  const sts = [];
  if (!pks) {
    pks = [];
    const ks = Object.keys(attrs);
    for (const k of ks) {
      const attr = attrs[k];
      attr.name = k;
      if (attr.key) {
        pks.push(attr);
      }
      if (attr.version) {
        ver = k;
      }
    }
  }
  for (const obj of objs) {
    const smt = buildToSave(obj, table, attrs, pks, ver, buildParam);
    if (smt.query) {
      sts.push(smt);
    }
  }
  return sts;
}
export function toString(v) {
  if (v === v && v !== Infinity && v !== -Infinity) {
    return "" + v;
  }
  return "null";
}
export class resource {
}
export class OracleTransaction {
  constructor(con) {
    this.con = con;
    this.completed = false;
    this.driver = "oracle";
    this.param = this.param.bind(this);
    this.execute = this.execute.bind(this);
    this.executeBatch = this.executeBatch.bind(this);
    this.query = this.query.bind(this);
    this.queryOne = this.queryOne.bind(this);
    this.executeScalar = this.executeScalar.bind(this);
    this.count = this.count.bind(this);
    this.ensureActive = this.ensureActive.bind(this);
    this.commit = this.commit.bind(this);
    this.rollback = this.rollback.bind(this);
  }
  ensureActive() {
    if (this.completed) {
      throw new Error("Transaction has already been completed");
    }
  }
  commit() {
    return __awaiter(this, void 0, void 0, function* () {
      this.ensureActive();
      this.completed = true;
      try {
        yield this.con.commit();
      }
      finally {
        yield this.con.close();
      }
    });
  }
  rollback() {
    return __awaiter(this, void 0, void 0, function* () {
      this.ensureActive();
      this.completed = true;
      try {
        yield this.con.rollback();
      }
      finally {
        yield this.con.close();
      }
    });
  }
  param(i) {
    return ":" + i;
  }
  execute(sql, args) {
    this.ensureActive();
    return executeTx(this.con, sql, args);
  }
  executeBatch(statements, requireFirstAffected) {
    this.ensureActive();
    return executeBatchTx(this.con, statements, requireFirstAffected);
  }
  query(sql, args, m, bools) {
    this.ensureActive();
    return queryTx(this.con, sql, args, m, bools);
  }
  queryOne(sql, args, m, bools) {
    this.ensureActive();
    return queryOneTx(this.con, sql, args, m, bools);
  }
  executeScalar(sql, args) {
    this.ensureActive();
    return executeScalarTx(this.con, sql, args);
  }
  count(sql, args) {
    this.ensureActive();
    return countTx(this.con, sql, args);
  }
}
export class OracleManager {
  constructor(pool) {
    this.pool = pool;
    this.driver = "oracle";
    this.param = this.param.bind(this);
    this.execute = this.execute.bind(this);
    this.executeBatch = this.executeBatch.bind(this);
    this.query = this.query.bind(this);
    this.queryOne = this.queryOne.bind(this);
    this.executeScalar = this.executeScalar.bind(this);
    this.count = this.count.bind(this);
    this.beginTransaction = this.beginTransaction.bind(this);
  }
  beginTransaction() {
    return __awaiter(this, void 0, void 0, function* () {
      const connection = yield this.pool.getConnection();
      const tx = new OracleTransaction(connection);
      return tx;
    });
  }
  param(i) {
    return ":" + i;
  }
  execute(sql, args) {
    return this.pool.getConnection().then((con) => execute(con, sql, args));
  }
  executeBatch(statements, requireFirstAffected) {
    return this.pool.getConnection().then((con) => executeBatch(con, statements, requireFirstAffected));
  }
  query(sql, args, m, bools) {
    return this.pool.getConnection().then((con) => query(con, sql, args, m, bools));
  }
  queryOne(sql, args, m, bools) {
    return this.pool.getConnection().then((con) => queryOne(con, sql, args, m, bools));
  }
  executeScalar(sql, args) {
    return this.pool.getConnection().then((con) => executeScalar(con, sql, args));
  }
  count(sql, args) {
    return this.pool.getConnection().then((con) => count(con, sql, args));
  }
}
export function executeBatch(con, statements, requireFirstAffected) {
  return __awaiter(this, void 0, void 0, function* () {
    if (!statements || statements.length === 0) {
      return 0;
    }
    let c = 0;
    try {
      if (requireFirstAffected) {
        const result0 = yield con.execute(statements[0].query, statements[0].params, { autoCommit: false });
        if (result0 && result0.rowsAffected && result0.rowsAffected > 0) {
          c += result0.rowsAffected;
          const l = statements.length;
          for (let j = 1; j < l; j++) {
            const item = statements[j];
            const res = yield con.execute(item.query, item.params ? item.params : [], { autoCommit: false });
            if (res.rowsAffected) {
              c += res.rowsAffected;
            }
          }
          yield con.commit();
          return c;
        }
        else {
          yield con.commit();
          return c;
        }
      }
      else {
        const l = statements.length;
        for (let j = 0; j < l; j++) {
          const item = statements[j];
          const res = yield con.execute(item.query, item.params ? item.params : [], { autoCommit: false });
          if (res.rowsAffected) {
            c += res.rowsAffected;
          }
        }
        yield con.commit();
        return c;
      }
    }
    catch (e) {
      try {
        yield con.rollback();
      }
      catch (e0) { }
      throw e;
    }
    finally {
      yield con.close();
    }
  });
}
export function executeBatchTx(con, statements, requireFirstAffected) {
  return __awaiter(this, void 0, void 0, function* () {
    if (!statements || statements.length === 0) {
      return 0;
    }
    let c = 0;
    try {
      if (requireFirstAffected) {
        const result0 = yield con.execute(statements[0].query, statements[0].params, { autoCommit: false });
        if (result0 && result0.rowsAffected && result0.rowsAffected > 0) {
          c += result0.rowsAffected;
          const l = statements.length;
          for (let j = 1; j < l; j++) {
            const item = statements[j];
            const res = yield con.execute(item.query, item.params ? item.params : [], { autoCommit: false });
            if (res.rowsAffected) {
              c += res.rowsAffected;
            }
          }
          return c;
        }
        else {
          return c;
        }
      }
      else {
        const l = statements.length;
        for (let j = 0; j < l; j++) {
          const item = statements[j];
          const res = yield con.execute(item.query, item.params ? item.params : [], { autoCommit: false });
          if (res.rowsAffected) {
            c += res.rowsAffected;
          }
        }
        return c;
      }
    }
    catch (e) {
      throw e;
    }
  });
}
export function executeTx(con, sql, args) {
  const p = toArray(args);
  return con.execute(sql, p, { autoCommit: false }).then((results) => { var _a; return (_a = results.rowsAffected) !== null && _a !== void 0 ? _a : 0; });
}
export function queryTx(con, sql, args, m, bools) {
  const p = toArray(args);
  return con.execute(sql, p, { autoCommit: false }).then((results) => {
    if (results.rows) {
      const x = results.metaData;
      if (!x) {
        return results.rows;
      }
      else {
        const arrayResult = results.rows.map((item) => {
          return formatData(x, item);
        });
        return handleResults(arrayResult, m, bools);
      }
    }
    else {
      return [];
    }
  });
}
export function queryOneTx(con, sql, args, m, bools) {
  return queryTx(con, sql, args, m, bools).then((r) => {
    return r && r.length > 0 ? r[0] : null;
  });
}
export function executeScalarTx(con, sql, args) {
  return queryOneTx(con, sql, args).then((r) => {
    if (!r) {
      return null;
    }
    else {
      const keys = Object.keys(r);
      return r[keys[0]];
    }
  });
}
export function countTx(con, sql, args) {
  return executeScalarTx(con, sql, args).then((res) => (res !== null ? res : 0));
}
export function execute(con, sql, args) {
  const p = toArray(args);
  return con
    .execute(sql, p)
    .then((results) => { var _a; return (_a = results.rowsAffected) !== null && _a !== void 0 ? _a : 0; })
    .finally(() => con.close());
}
export function query(con, sql, args, m, bools) {
  const p = toArray(args);
  return con
    .execute(sql, p)
    .then((results) => {
    if (results.rows) {
      const x = results.metaData;
      if (!x) {
        return results.rows;
      }
      else {
        const arrayResult = results.rows.map((item) => {
          return formatData(x, item);
        });
        return handleResults(arrayResult, m, bools);
      }
    }
    else {
      return [];
    }
  })
    .finally(() => con.close());
}
export function queryOne(con, sql, args, m, bools) {
  return query(con, sql, args, m, bools).then((r) => {
    return r && r.length > 0 ? r[0] : null;
  });
}
export function executeScalar(con, sql, args) {
  return queryOne(con, sql, args).then((r) => {
    if (!r) {
      return null;
    }
    else {
      const keys = Object.keys(r);
      return r[keys[0]];
    }
  });
}
export function count(con, sql, args) {
  return executeScalar(con, sql, args).then((res) => (res !== null ? res : 0));
}
export function insertBatch(con, objs, table, attrs, ver, notSkipInvalid, buildParam) {
  const s = buildToInsertBatch(objs, table, attrs, ver, notSkipInvalid, buildParam);
  if (!s.query) {
    return Promise.resolve(-1);
  }
  if (typeof con === "function") {
    return con(s.query, s.params);
  }
  else {
    return execute(con, s.query, s.params);
  }
}
export function toArray(arr) {
  if (!arr || arr.length === 0) {
    return [];
  }
  const p = [];
  const l = arr.length;
  for (let i = 0; i < l; i++) {
    if (arr[i] === undefined || arr[i] == null) {
      p.push(null);
    }
    else {
      if (typeof arr[i] === "object") {
        if (arr[i] instanceof Date) {
          p.push(arr[i]);
        }
        else {
          if (resource.string) {
            const s = JSON.stringify(arr[i]);
            p.push(s);
          }
          else {
            p.push(arr[i]);
          }
        }
      }
      else {
        p.push(arr[i]);
      }
    }
  }
  return p;
}
export function handleResults(r, m, bools) {
  if (m) {
    const res = mapArray(r, m);
    if (bools && bools.length > 0) {
      return handleBool(res, bools);
    }
    else {
      return res;
    }
  }
  else {
    if (bools && bools.length > 0) {
      return handleBool(r, bools);
    }
    else {
      return r;
    }
  }
}
export function handleBool(objs, bools) {
  if (!bools || bools.length === 0 || !objs) {
    return objs;
  }
  for (const obj of objs) {
    const o = obj;
    for (const field of bools) {
      if (field.name) {
        const v = o[field.name];
        if (typeof v !== "boolean" && v != null && v !== undefined) {
          const b = field.true;
          if (b == null) {
            o[field.name] = "1" == v || "T" == v || "Y" == v || "true" == v;
          }
          else {
            o[field.name] = v == b ? true : false;
          }
        }
      }
    }
  }
  return objs;
}
export function map(obj, m) {
  if (!m) {
    return obj;
  }
  const mkeys = Object.keys(m);
  if (mkeys.length === 0) {
    return obj;
  }
  const obj2 = {};
  const keys = Object.keys(obj);
  for (const key of keys) {
    let k0 = m[key];
    if (!k0) {
      k0 = key;
    }
    obj2[k0] = obj[key];
  }
  return obj2;
}
export function mapArray(results, m) {
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
    const obj2 = {};
    const keys = Object.keys(obj);
    for (const key of keys) {
      let k0 = m[key];
      if (!k0) {
        k0 = key;
      }
      obj2[k0] = obj[key];
    }
    objs.push(obj2);
  }
  return objs;
}
export function getFields(fields, all) {
  if (!fields || fields.length === 0) {
    return undefined;
  }
  const ext = [];
  if (all) {
    for (const s of fields) {
      if (all.includes(s)) {
        ext.push(s);
      }
    }
    if (ext.length === 0) {
      return undefined;
    }
    else {
      return ext;
    }
  }
  else {
    return fields;
  }
}
export function buildFields(fields, all) {
  const s = getFields(fields, all);
  if (!s || s.length === 0) {
    return "*";
  }
  else {
    return s.join(",");
  }
}
export function getMapField(name, mp) {
  if (!mp) {
    return name;
  }
  const x = mp[name];
  if (!x) {
    return name;
  }
  if (typeof x === "string") {
    return x;
  }
  return name;
}
export function isEmpty(s) {
  return !(s && s.length > 0);
}
export function formatData(nameColumn, data, m) {
  const result = {};
  nameColumn.forEach((item, index) => {
    var _a;
    const key = (_a = m === null || m === void 0 ? void 0 : m[item.name]) !== null && _a !== void 0 ? _a : item.name;
    result[key] = data[index];
  });
  return result;
}
export function version(attrs) {
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
export class OracleBatchInserter {
  constructor(connection, table, attributes, map, notSkipInvalid, buildVersion, buildParam) {
    this.connection = connection;
    this.table = table;
    this.attributes = attributes;
    this.map = map;
    this.notSkipInvalid = notSkipInvalid;
    this.buildVersion = buildVersion;
    this.write = this.write.bind(this);
    this.param = buildParam ? buildParam : param;
    if (buildVersion) {
      const x = version(attributes);
      if (x) {
        this.version = x.name;
      }
    }
  }
  write(objs) {
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
    if (stmt.query) {
      return execute(this.connection, stmt.query, stmt.params);
    }
    else {
      return Promise.resolve(0);
    }
  }
}
export class OracleWriter {
  constructor(connection, table, attributes, oneIfSuccess, map, buildParam) {
    this.connection = connection;
    this.table = table;
    this.attributes = attributes;
    this.oneIfSuccess = oneIfSuccess;
    this.map = map;
    this.write = this.write.bind(this);
    this.param = buildParam ? buildParam : param;
    const m = metadata(attributes);
    this.keys = m.keys;
    this.version = m.version;
  }
  write(obj) {
    if (!obj) {
      return Promise.resolve(0);
    }
    let obj2 = obj;
    if (this.map) {
      obj2 = this.map(obj);
    }
    const stmt = buildToSave(obj2, this.table, this.attributes, this.keys, this.version, this.param);
    if (stmt.query) {
      if (this.oneIfSuccess) {
        return execute(this.connection, stmt.query, stmt.params).then((ct) => (ct > 0 ? 1 : 0));
      }
      else {
        return execute(this.connection, stmt.query, stmt.params);
      }
    }
    else {
      return Promise.resolve(0);
    }
  }
}
export class OracleBufferedBatchWriter {
  constructor(pool, table, attributes, size = 5000, map, buildParam) {
    this.pool = pool;
    this.table = table;
    this.attributes = attributes;
    this.size = size;
    this.map = map;
    this.list = [];
    this.write = this.write.bind(this);
    this.flush = this.flush.bind(this);
    this.param = buildParam;
    const m = metadata(attributes);
    this.keys = m.keys;
    this.version = m.version;
  }
  write(obj) {
    if (!obj) {
      return Promise.resolve(0);
    }
    let obj2 = obj;
    if (this.map) {
      obj2 = this.map(obj);
      this.list.push(obj2);
    }
    else {
      this.list.push(obj);
    }
    if (this.list.length < this.size) {
      return Promise.resolve(0);
    }
    else {
      return this.flush();
    }
  }
  flush() {
    if (!this.list || this.list.length === 0) {
      return Promise.resolve(0);
    }
    else {
      const stmts = buildToSaveBatch(this.list, this.table, this.attributes, this.keys, this.version, this.param);
      if (stmts && stmts.length > 0) {
        return this.pool.getConnection().then((connection) => {
          return executeBatch(connection, stmts).then((r) => {
            this.list = [];
            return r;
          });
        });
      }
      else {
        this.list = [];
        return Promise.resolve(0);
      }
    }
  }
}
export class OracleBatchWriter {
  constructor(connection, table, attributes, map, buildParam) {
    this.connection = connection;
    this.table = table;
    this.attributes = attributes;
    this.map = map;
    this.write = this.write.bind(this);
    this.param = buildParam ? buildParam : param;
    const m = metadata(attributes);
    this.keys = m.keys;
    this.version = m.version;
  }
  write(objs) {
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
    const stmts = buildToSaveBatch(list, this.table, this.attributes, this.keys, this.version, this.param);
    if (stmts && stmts.length > 0) {
      return executeBatch(this.connection, stmts);
    }
    else {
      return Promise.resolve(0);
    }
  }
}
export class OracleChecker {
  constructor(pool, checkerName = "oracle", timeout = 4500) {
    this.pool = pool;
    this.checkerName = checkerName;
    this.timeout = timeout;
  }
  name() {
    return this.checkerName;
  }
  build(data, error) {
    var _a;
    return Object.assign(Object.assign({ name: this.name(), status: "DOWN" }, data), { error: (_a = error === null || error === void 0 ? void 0 : error.message) !== null && _a !== void 0 ? _a : error });
  }
  check() {
    return __awaiter(this, void 0, void 0, function* () {
      let connection;
      try {
        connection = yield this.pool.getConnection();
        connection.callTimeout = this.timeout;
        yield connection.execute("SELECT 1 FROM DUAL");
        return {
          name: this.name(),
          status: "UP",
        };
      }
      catch (error) {
        return this.build({}, error);
      }
      finally {
        if (connection) {
          try {
            yield connection.close();
          }
          catch (_a) {
          }
        }
      }
    });
  }
}
export class Exporter {
  constructor(connection, filename, attributes, buildQuery, format, write, end, logInfo, progressSize = 10000, isClose = true) {
    this.connection = connection;
    this.filename = filename;
    this.attributes = attributes;
    this.buildQuery = buildQuery;
    this.format = format;
    this.write = write;
    this.end = end;
    this.logInfo = logInfo;
    this.progressSize = progressSize;
    this.isClose = isClose;
    this.export = this.export.bind(this);
  }
  export(ctx) {
    return __awaiter(this, void 0, void 0, function* () {
      const stmt = yield this.buildQuery(ctx);
      const stream = this.connection.queryStream(stmt.query, stmt.params || {});
      return new Promise((resolve, reject) => {
        let metaData;
        let i = 0;
        let j = 0;
        let errorHandled = false;
        stream.on("metadata", (metadata) => (metaData = metadata));
        stream.on("data", (row) => {
          i++;
          j++;
          const obj = convertToObject(row, metaData, this.attributes);
          const exportStr = this.format(obj);
          this.write(exportStr);
          if (j >= this.progressSize) {
            if (this.logInfo) {
              this.logInfo(`Progress: ${i} records processed of file '${this.filename}'`);
            }
            j = 0;
          }
        });
        stream.on("error", (error) => __awaiter(this, void 0, void 0, function* () {
          if (errorHandled) {
            return;
          }
          errorHandled = true;
          try {
            if (this.isClose) {
              yield closeConnection(this.connection);
            }
          }
          finally {
            reject(error);
          }
        }));
        stream.on("end", () => __awaiter(this, void 0, void 0, function* () {
          if (errorHandled) {
            return;
          }
          try {
            stream.destroy();
            this.end();
            if (this.isClose) {
              yield closeConnection(this.connection);
            }
            resolve(i);
          }
          catch (error) {
            reject(error);
          }
        }));
      });
    });
  }
}
export class ExportService {
  constructor(connection, filename, attributes, queryBuilder, formatter, writer, logInfo, progressSize = 10000, isClose = true) {
    this.connection = connection;
    this.filename = filename;
    this.attributes = attributes;
    this.queryBuilder = queryBuilder;
    this.formatter = formatter;
    this.writer = writer;
    this.logInfo = logInfo;
    this.progressSize = progressSize;
    this.isClose = isClose;
    this.export = this.export.bind(this);
  }
  export(ctx) {
    return __awaiter(this, void 0, void 0, function* () {
      const stmt = yield this.queryBuilder.build(ctx);
      const stream = this.connection.queryStream(stmt.query, stmt.params || {});
      return new Promise((resolve, reject) => {
        let metaData;
        let i = 0;
        let j = 0;
        let errorHandled = false;
        stream.on("metadata", (metadata) => (metaData = metadata));
        stream.on("data", (row) => {
          i++;
          j++;
          const obj = convertToObject(row, metaData, this.attributes);
          const exportStr = this.formatter.format(obj);
          this.writer.write(exportStr);
          if (j >= this.progressSize) {
            if (this.logInfo) {
              this.logInfo(`Progress: ${i} records processed of file '${this.filename}'`);
            }
            j = 0;
          }
        });
        stream.on("error", (error) => __awaiter(this, void 0, void 0, function* () {
          if (errorHandled) {
            return;
          }
          errorHandled = true;
          try {
            if (this.isClose) {
              yield closeConnection(this.connection);
            }
          }
          finally {
            reject(error);
          }
        }));
        stream.on("end", () => __awaiter(this, void 0, void 0, function* () {
          if (errorHandled) {
            return;
          }
          try {
            stream.destroy();
            if (this.writer.end) {
              this.writer.end();
            }
            if (this.isClose) {
              yield closeConnection(this.connection);
            }
            resolve(i);
          }
          catch (error) {
            reject(error);
          }
        }));
      });
    });
  }
}
function closeConnection(connection) {
  return __awaiter(this, void 0, void 0, function* () {
    if (!connection) {
      return;
    }
    try {
      yield connection.close();
    }
    catch (err) {
      console.error(err);
    }
  });
}
function convertToObject(row, metadata, attributes) {
  const rsl = {};
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
export function select(table, attrs) {
  const cols = [];
  const ks = Object.keys(attrs);
  for (const k of ks) {
    const attr = attrs[k];
    attr.name = k;
    const field = attr.column ? attr.column : k;
    cols.push(field);
  }
  return `select ${cols.join(",")} from ${table}`;
}
