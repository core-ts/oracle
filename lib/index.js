"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
  function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
  return new (P || (P = Promise))(function (resolve, reject) {
    function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
    function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
    function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
    step((generator = generator.apply(thisArg, _arguments || [])).next());
  });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
  var _ = { label: 0, sent: function () { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
  return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function () { return this; }), g;
  function verb(n) { return function (v) { return step([n, v]); }; }
  function step(op) {
    if (f) throw new TypeError("Generator is already executing.");
    while (_) try {
      if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
      if (y = 0, t) op = [op[0] & 2, t.value];
      switch (op[0]) {
        case 0: case 1: t = op; break;
        case 4: _.label++; return { value: op[1], done: false };
        case 5: _.label++; y = op[1]; op = [0]; continue;
        case 7: op = _.ops.pop(); _.trys.pop(); continue;
        default:
          if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
          if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
          if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
          if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
          if (t[2]) _.ops.pop();
          _.trys.pop(); continue;
      }
      op = body.call(thisArg, _);
    } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
    if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
  }
};
function __export(m) {
  for (var p in m) if (!exports.hasOwnProperty(p)) exports[p] = m[p];
}
Object.defineProperty(exports, "__esModule", { value: true });
var build_1 = require("./build");
__export(require("./build"));
__export(require("./checker"));
var resource = (function () {
  function resource() {
  }
  return resource;
}());
exports.resource = resource;
var OracleManager = (function () {
  function OracleManager(conn) {
    this.conn = conn;
    this.driver = 'oracle';
    this.param = this.param.bind(this);
    this.exec = this.exec.bind(this);
    this.execBatch = this.execBatch.bind(this);
    this.query = this.query.bind(this);
    this.queryOne = this.queryOne.bind(this);
    this.execScalar = this.execScalar.bind(this);
    this.count = this.count.bind(this);
  }
  OracleManager.prototype.param = function (i) {
    return ':' + i;
  };
  OracleManager.prototype.exec = function (sql, args, ctx) {
    var p = (ctx ? ctx : this.conn);
    return exec(p, sql, args);
  };
  OracleManager.prototype.execBatch = function (statements, firstSuccess, ctx) {
    var p = (ctx ? ctx : this.conn);
    return execBatch(p, statements, firstSuccess);
  };
  OracleManager.prototype.query = function (sql, args, m, bools, ctx) {
    var p = (ctx ? ctx : this.conn);
    return query(p, sql, args, m, bools);
  };
  OracleManager.prototype.queryOne = function (sql, args, m, bools, ctx) {
    var p = (ctx ? ctx : this.conn);
    return queryOne(p, sql, args, m, bools);
  };
  OracleManager.prototype.execScalar = function (sql, args, ctx) {
    var p = (ctx ? ctx : this.conn);
    return execScalar(p, sql, args);
  };
  OracleManager.prototype.count = function (sql, args, ctx) {
    var p = (ctx ? ctx : this.conn);
    return count(p, sql, args);
  };
  return OracleManager;
}());
exports.OracleManager = OracleManager;
function execBatch(conn, statements, firstSuccess) {
  return __awaiter(this, void 0, void 0, function () {
    var c, result0, subs, arrPromise, results, _i, results_1, obj, arrPromise, results, _a, results_2, obj, e_1;
    return __generator(this, function (_b) {
      switch (_b.label) {
        case 0:
          if (!statements || statements.length === 0) {
            return [2, Promise.resolve(0)];
          }
          else if (statements.length === 1) {
            return [2, exec(conn, statements[0].query, statements[0].params)];
          }
          c = 0;
          _b.label = 1;
        case 1:
          _b.trys.push([1, 12, 14, 15]);
          if (!firstSuccess) return [3, 8];
          return [4, conn.execute(statements[0].query, statements[0].params, { autoCommit: false })];
        case 2:
          result0 = _b.sent();
          if (!(result0 && result0.rowsAffected && result0.rowsAffected > 0)) return [3, 5];
          subs = statements.slice(1);
          arrPromise = subs.map(function (item) {
            return conn.execute(item.query, item.params ? item.params : [], { autoCommit: false });
          });
          return [4, Promise.all(arrPromise)];
        case 3:
          results = _b.sent();
          for (_i = 0, results_1 = results; _i < results_1.length; _i++) {
            obj = results_1[_i];
            if (obj.rowsAffected) {
              c += obj.rowsAffected;
            }
          }
          if (result0.rowsAffected) {
            c += result0.rowsAffected;
          }
          return [4, conn.commit()];
        case 4:
          _b.sent();
          return [2, c];
        case 5: return [4, conn.commit()];
        case 6:
          _b.sent();
          return [2, c];
        case 7: return [3, 11];
        case 8:
          arrPromise = statements.map(function (item) { return conn.execute(item.query, item.params ? item.params : [], { autoCommit: false }); });
          return [4, Promise.all(arrPromise)];
        case 9:
          results = _b.sent();
          for (_a = 0, results_2 = results; _a < results_2.length; _a++) {
            obj = results_2[_a];
            if (obj.rowsAffected) {
              c += obj.rowsAffected;
            }
          }
          return [4, conn.commit()];
        case 10:
          _b.sent();
          return [2, c];
        case 11: return [3, 15];
        case 12:
          e_1 = _b.sent();
          return [4, conn.rollback()];
        case 13:
          _b.sent();
          console.log(e_1);
          throw e_1;
        case 14:
          conn.release();
          return [7];
        case 15: return [2];
      }
    });
  });
}
exports.execBatch = execBatch;
function exec(conn, sql, args) {
  var p = toArray(args);
  return new Promise(function (resolve, reject) {
    return conn.execute(sql, p, function (err, results) {
      if (err) {
        console.log(err);
        return reject(err);
      }
      else {
        if (results.rowsAffected) {
          return resolve(results.rowsAffected);
        }
        else {
          return resolve(-1);
        }
      }
    });
  });
}
exports.exec = exec;
function query(conn, sql, args, m, bools) {
  var p = toArray(args);
  return new Promise(function (resolve, reject) {
    return conn.execute(sql, p, function (err, results) {
      if (err) {
        return reject(err);
      }
      else {
        if (results.rows) {
          var x_1 = results.metaData;
          if (!x_1) {
            return resolve(results.rows);
          }
          else {
            var arrayResult = results.rows.map(function (item) {
              return formatData(x_1, item);
            });
            return resolve(handleResults(arrayResult, m, bools));
          }
        }
        else {
          return resolve([]);
        }
      }
    });
  });
}
exports.query = query;
function queryOne(conn, sql, args, m, bools) {
  return query(conn, sql, args, m, bools).then(function (r) {
    return (r && r.length > 0 ? r[0] : null);
  });
}
exports.queryOne = queryOne;
function execScalar(conn, sql, args) {
  return queryOne(conn, sql, args).then(function (r) {
    if (!r) {
      return null;
    }
    else {
      var keys = Object.keys(r);
      return r[keys[0]];
    }
  });
}
exports.execScalar = execScalar;
function count(conn, sql, args) {
  return execScalar(conn, sql, args);
}
exports.count = count;
function insertBatch(conn, objs, table, attrs, ver, notSkipInvalid, buildParam) {
  var s = build_1.buildToInsertBatch(objs, table, attrs, ver, notSkipInvalid, buildParam);
  if (!s) {
    return Promise.resolve(-1);
  }
  if (typeof conn === 'function') {
    return conn(s.query, s.params);
  }
  else {
    return exec(conn, s.query, s.params);
  }
}
exports.insertBatch = insertBatch;
function save(conn, obj, table, attrs, ver, buildParam, i) {
  var s = build_1.buildToSave(obj, table, attrs, ver, buildParam);
  if (!s) {
    return Promise.resolve(-1);
  }
  if (typeof conn === 'function') {
    return conn(s.query, s.params);
  }
  else {
    return exec(conn, s.query, s.params);
  }
}
exports.save = save;
function saveBatch(conn, objs, table, attrs, ver, buildParam) {
  var s = build_1.buildToSaveBatch(objs, table, attrs, ver, buildParam);
  if (typeof conn === 'function') {
    return conn(s);
  }
  else {
    return execBatch(conn, s);
  }
}
exports.saveBatch = saveBatch;
function toArray(arr) {
  if (!arr || arr.length === 0) {
    return [];
  }
  var p = [];
  var l = arr.length;
  for (var i = 0; i < l; i++) {
    if (arr[i] === undefined || arr[i] == null) {
      p.push(null);
    }
    else {
      if (typeof arr[i] === 'object') {
        if (arr[i] instanceof Date) {
          p.push(arr[i]);
        }
        else {
          if (resource.string) {
            var s = JSON.stringify(arr[i]);
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
exports.toArray = toArray;
function handleResults(r, m, bools) {
  if (m) {
    var res = mapArray(r, m);
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
exports.handleResults = handleResults;
function handleBool(objs, bools) {
  if (!bools || bools.length === 0 || !objs) {
    return objs;
  }
  for (var _i = 0, objs_1 = objs; _i < objs_1.length; _i++) {
    var obj = objs_1[_i];
    var o = obj;
    for (var _a = 0, bools_1 = bools; _a < bools_1.length; _a++) {
      var field = bools_1[_a];
      if (field.name) {
        var v = o[field.name];
        if (typeof v !== 'boolean' && v != null && v !== undefined) {
          var b = field.true;
          if (b == null || b === undefined) {
            o[field.name] = ('1' == v || 'T' == v || 'Y' == v || 'true' == v);
          }
          else {
            o[field.name] = (v == b ? true : false);
          }
        }
      }
    }
  }
  return objs;
}
exports.handleBool = handleBool;
function map(obj, m) {
  if (!m) {
    return obj;
  }
  var mkeys = Object.keys(m);
  if (mkeys.length === 0) {
    return obj;
  }
  var obj2 = {};
  var keys = Object.keys(obj);
  for (var _i = 0, keys_1 = keys; _i < keys_1.length; _i++) {
    var key = keys_1[_i];
    var k0 = m[key];
    if (!k0) {
      k0 = key;
    }
    obj2[k0] = obj[key];
  }
  return obj2;
}
exports.map = map;
function mapArray(results, m) {
  if (!m) {
    return results;
  }
  var mkeys = Object.keys(m);
  if (mkeys.length === 0) {
    return results;
  }
  var objs = [];
  var length = results.length;
  for (var i = 0; i < length; i++) {
    var obj = results[i];
    var obj2 = {};
    var keys = Object.keys(obj);
    for (var _i = 0, keys_2 = keys; _i < keys_2.length; _i++) {
      var key = keys_2[_i];
      var k0 = m[key];
      if (!k0) {
        k0 = key;
      }
      obj2[k0] = obj[key];
    }
    objs.push(obj2);
  }
  return objs;
}
exports.mapArray = mapArray;
function getFields(fields, all) {
  if (!fields || fields.length === 0) {
    return undefined;
  }
  var ext = [];
  if (all) {
    for (var _i = 0, fields_1 = fields; _i < fields_1.length; _i++) {
      var s = fields_1[_i];
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
exports.getFields = getFields;
function buildFields(fields, all) {
  var s = getFields(fields, all);
  if (!s || s.length === 0) {
    return '*';
  }
  else {
    return s.join(',');
  }
}
exports.buildFields = buildFields;
function getMapField(name, mp) {
  if (!mp) {
    return name;
  }
  var x = mp[name];
  if (!x) {
    return name;
  }
  if (typeof x === 'string') {
    return x;
  }
  return name;
}
exports.getMapField = getMapField;
function isEmpty(s) {
  return !(s && s.length > 0);
}
exports.isEmpty = isEmpty;
function formatData(nameColumn, data, m) {
  var result = {};
  nameColumn.forEach(function (item, index) {
    var key = item.name;
    if (m) {
      key = m[item.name];
    }
    result[key] = data[index];
  });
  return result;
}
exports.formatData = formatData;
function version(attrs) {
  var ks = Object.keys(attrs);
  for (var _i = 0, ks_1 = ks; _i < ks_1.length; _i++) {
    var k = ks_1[_i];
    var attr = attrs[k];
    if (attr.version) {
      attr.name = k;
      return attr;
    }
  }
  return undefined;
}
exports.version = version;
var OracleBatchInserter = (function () {
  function OracleBatchInserter(conn, table, attributes, toDB, notSkipInvalid, buildParam) {
    this.table = table;
    this.attributes = attributes;
    this.notSkipInvalid = notSkipInvalid;
    this.write = this.write.bind(this);
    if (typeof conn === 'function') {
      this.exec = conn;
    }
    else {
      this.connection = conn;
    }
    this.param = buildParam;
    this.map = toDB;
    var x = version(attributes);
    if (x) {
      this.version = x.name;
    }
  }
  OracleBatchInserter.prototype.write = function (objs) {
    if (!objs || objs.length === 0) {
      return Promise.resolve(0);
    }
    var list = objs;
    if (this.map) {
      list = [];
      for (var _i = 0, objs_2 = objs; _i < objs_2.length; _i++) {
        var obj = objs_2[_i];
        var obj2 = this.map(obj);
        list.push(obj2);
      }
    }
    var stmt = build_1.buildToInsertBatch(list, this.table, this.attributes, this.version, this.notSkipInvalid, this.param);
    if (stmt) {
      if (this.exec) {
        return this.exec(stmt.query, stmt.params);
      }
      else {
        return exec(this.connection, stmt.query, stmt.params);
      }
    }
    else {
      return Promise.resolve(0);
    }
  };
  return OracleBatchInserter;
}());
exports.OracleBatchInserter = OracleBatchInserter;
var OracleWriter = (function () {
  function OracleWriter(conn, table, attributes, toDB, buildParam) {
    this.table = table;
    this.attributes = attributes;
    this.write = this.write.bind(this);
    if (typeof conn === 'function') {
      this.exec = conn;
    }
    else {
      this.connection = conn;
    }
    this.param = buildParam;
    this.map = toDB;
    var x = version(attributes);
    if (x) {
      this.version = x.name;
    }
  }
  OracleWriter.prototype.write = function (obj) {
    if (!obj) {
      return Promise.resolve(0);
    }
    var obj2 = obj;
    if (this.map) {
      obj2 = this.map(obj);
    }
    var stmt = build_1.buildToSave(obj2, this.table, this.attributes, this.version, this.param);
    if (stmt) {
      if (this.exec) {
        return this.exec(stmt.query, stmt.params);
      }
      else {
        return exec(this.connection, stmt.query, stmt.params);
      }
    }
    else {
      return Promise.resolve(0);
    }
  };
  return OracleWriter;
}());
exports.OracleWriter = OracleWriter;
var OracleBatchWriter = (function () {
  function OracleBatchWriter(conn, table, attributes, toDB, buildParam) {
    this.table = table;
    this.attributes = attributes;
    this.write = this.write.bind(this);
    if (typeof conn === 'function') {
      this.execute = conn;
    }
    else {
      this.connection = conn;
    }
    this.param = buildParam;
    this.map = toDB;
    var x = version(attributes);
    if (x) {
      this.version = x.name;
    }
  }
  OracleBatchWriter.prototype.write = function (objs) {
    if (!objs || objs.length === 0) {
      return Promise.resolve(0);
    }
    var list = objs;
    if (this.map) {
      list = [];
      for (var _i = 0, objs_3 = objs; _i < objs_3.length; _i++) {
        var obj = objs_3[_i];
        var obj2 = this.map(obj);
        list.push(obj2);
      }
    }
    var stmts = build_1.buildToSaveBatch(list, this.table, this.attributes, this.version, this.param);
    if (stmts && stmts.length > 0) {
      if (this.execute) {
        return this.execute(stmts);
      }
      else {
        return execBatch(this.connection, stmts);
      }
    }
    else {
      return Promise.resolve(0);
    }
  };
  return OracleBatchWriter;
}());
exports.OracleBatchWriter = OracleBatchWriter;
