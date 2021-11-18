"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
function param(i) {
  return ':' + i;
}
exports.param = param;
function params(length, from) {
  if (from === undefined || from == null) {
    from = 0;
  }
  var ps = [];
  for (var i = 1; i <= length; i++) {
    ps.push(param(i + from));
  }
  return ps;
}
exports.params = params;
function metadata(attrs) {
  var mp = {};
  var ks = Object.keys(attrs);
  var ats = [];
  var bools = [];
  var fields = [];
  var m = { keys: ats, fields: fields };
  var isMap = false;
  for (var _i = 0, ks_1 = ks; _i < ks_1.length; _i++) {
    var k = ks_1[_i];
    var attr = attrs[k];
    attr.name = k;
    if (attr.key) {
      ats.push(attr);
    }
    if (!attr.ignored) {
      fields.push(k);
    }
    if (attr.type === 'boolean') {
      bools.push(attr);
    }
    if (attr.version) {
      m.version = k;
    }
    var field = (attr.field ? attr.field : k);
    var s = field.toLowerCase();
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
exports.metadata = metadata;
function buildToInsertBatch(objs, table, attrs, ver, notSkipInvalid, buildParam) {
  if (!buildParam) {
    buildParam = param;
  }
  var i = 1;
  var ks = Object.keys(attrs);
  var args = [];
  var rows = [];
  for (var _i = 0, objs_1 = objs; _i < objs_1.length; _i++) {
    var obj = objs_1[_i];
    var cols = [];
    var values = [];
    var isVersion = false;
    for (var _a = 0, ks_2 = ks; _a < ks_2.length; _a++) {
      var k = ks_2[_a];
      var v = obj[k];
      var attr = attrs[k];
      if (attr && !attr.ignored && !attr.noinsert) {
        if (v === undefined || v == null) {
          v = attr.default;
        }
        if (v !== undefined && v != null) {
          var field = (attr.field ? attr.field : k);
          cols.push(field);
          if (k === ver) {
            isVersion = true;
            values.push("" + 1);
          }
          else {
            if (v === '') {
              values.push("''");
            }
            else if (typeof v === 'number') {
              values.push(toString(v));
            }
            else if (typeof v === 'boolean') {
              if (attr.true === undefined) {
                if (v === true) {
                  values.push("true");
                }
                else {
                  values.push("false");
                }
              }
              else {
                var p = buildParam(i++);
                values.push(p);
                if (v === true) {
                  var v2 = (attr.true ? attr.true : '1');
                  args.push(v2);
                }
                else {
                  var v2 = (attr.false ? attr.false : '0');
                  args.push(v2);
                }
              }
            }
            else {
              var p = buildParam(i++);
              values.push(p);
              args.push(v);
            }
          }
        }
      }
    }
    if (!isVersion && ver && ver.length > 0) {
      var attr = attrs[ver];
      var field = (attr.field ? attr.field : ver);
      cols.push(field);
      values.push("" + 1);
    }
    if (cols.length === 0) {
      if (notSkipInvalid) {
        return undefined;
      }
    }
    else {
      var s = "into " + table + "(" + cols.join(',') + ")values(" + values.join(',') + ")";
      rows.push(s);
    }
  }
  if (rows.length === 0) {
    return undefined;
  }
  var query = "insert all " + rows.join(' ') + " select * from dual";
  return { query: query, params: args };
}
exports.buildToInsertBatch = buildToInsertBatch;
function buildToSave(obj, table, attrs, ver, buildParam, pks, i) {
  if (!i) {
    i = 1;
  }
  if (!buildParam) {
    buildParam = param;
  }
  var cols = [];
  var values = [];
  var args = [];
  var isVersion = false;
  var ks = Object.keys(attrs);
  if (!pks) {
    pks = [];
    for (var _i = 0, ks_3 = ks; _i < ks_3.length; _i++) {
      var k = ks_3[_i];
      var attr = attrs[k];
      attr.name = k;
      if (attr.key) {
        pks.push(attr);
      }
    }
  }
  var colQuery = [];
  var colSet = [];
  var noUpdate = false;
  if (pks.length > 0) {
    for (var _a = 0, pks_1 = pks; _a < pks_1.length; _a++) {
      var pk = pks_1[_a];
      if (pk.name) {
        var v = obj[pk.name];
        if (!v) {
          return undefined;
        }
        else {
          var attr = attrs[pk.name];
          var field = (attr.field ? attr.field : pk.name);
          var x = void 0;
          if (v == null) {
            x = 'null';
            noUpdate = true;
          }
          else if (v === '') {
            x = "''";
          }
          else if (typeof v === 'number') {
            x = toString(v);
          }
          else {
            x = buildParam(i++);
            if (typeof v === 'boolean') {
              if (v === true) {
                var v2 = (attr.true ? '' + attr.true : "'1'");
                args.push(v2);
              }
              else {
                var v2 = (attr.false ? '' + attr.false : "'0'");
                args.push(v2);
              }
            }
            else {
              args.push(v);
            }
          }
          colQuery.push(field + "=" + x);
        }
      }
    }
    for (var _b = 0, ks_4 = ks; _b < ks_4.length; _b++) {
      var k = ks_4[_b];
      var v = obj[k];
      if (v !== undefined) {
        var attr = attrs[k];
        if (!attr.key && !attr.ignored && k !== ver && !attr.noupdate) {
          var field = (attr.field ? attr.field : k);
          var x = void 0;
          if (v == null) {
            x = 'null';
          }
          else if (v === '') {
            x = "''";
          }
          else if (typeof v === 'number') {
            x = toString(v);
          }
          else if (typeof v === 'boolean') {
            if (attr.true === undefined) {
              if (v === true) {
                x = "'1'";
              }
              else {
                x = "'0'";
              }
            }
            else {
              x = buildParam(i++);
              if (v === true) {
                var v2 = (attr.true ? attr.true : "'1'");
                args.push(v2);
              }
              else {
                var v2 = (attr.false ? attr.false : "'0'");
                args.push(v2);
              }
            }
          }
          else {
            x = buildParam(i++);
            args.push(v);
          }
          colSet.push(field + "=" + x);
        }
      }
    }
  }
  for (var _c = 0, ks_5 = ks; _c < ks_5.length; _c++) {
    var k = ks_5[_c];
    var attr = attrs[k];
    var v = obj[k];
    if (v === undefined || v == null) {
      v = attr.default;
    }
    if (v !== undefined && v != null && !attr.ignored && !attr.noinsert) {
      var field = (attr.field ? attr.field : k);
      cols.push(field);
      if (k === ver) {
        isVersion = true;
        values.push("" + 1);
      }
      else {
        if (v === '') {
          values.push("''");
        }
        else if (typeof v === 'number') {
          values.push(toString(v));
        }
        else if (typeof v === 'boolean') {
          if (attr.true === undefined) {
            if (v === true) {
              values.push("true");
            }
            else {
              values.push("false");
            }
          }
          else {
            var p = buildParam(i++);
            values.push(p);
            if (v === true) {
              var v2 = (attr.true ? attr.true : "'1'");
              args.push(v2);
            }
            else {
              var v2 = (attr.false ? attr.false : "'0'");
              args.push(v2);
            }
          }
        }
        else {
          var p = buildParam(i++);
          values.push(p);
          args.push(v);
        }
      }
    }
  }
  if (pks.length === 0 && cols.length === 0) {
    return undefined;
  }
  if (!isVersion && ver && ver.length > 0) {
    var attr = attrs[ver];
    var field = (attr.field ? attr.field : ver);
    cols.push(field);
    values.push("" + 1);
  }
  if (noUpdate || pks.length === 0 || colSet.length === 0) {
    var q = "insert into " + table + "(" + cols.join(',') + ")values(" + values.join(',') + ")";
    return { query: q, params: args };
  }
  else {
    if (ver && ver.length > 0) {
      var v = obj[ver];
      if (typeof v === 'number' && !isNaN(v)) {
        var attr = attrs[ver];
        if (attr) {
          var field = (attr.field ? attr.field : ver);
          colSet.push(field + "=" + (1 + v));
          colQuery.push(field + "=" + v);
        }
      }
    }
    var query = "merge into " + table + " using dual on (" + colQuery.join(' and ') + ")\n    when matched then update set " + colSet.join(',') + "\n    when not matched then insert (" + cols.join(',') + ")\n        values (" + values.join(',') + ")\n";
    return { query: query, params: args };
  }
}
exports.buildToSave = buildToSave;
function buildToSaveBatch(objs, table, attrs, ver, buildParam) {
  if (!buildParam) {
    buildParam = param;
  }
  var sts = [];
  var ks = Object.keys(attrs);
  var pks = [];
  for (var _i = 0, ks_6 = ks; _i < ks_6.length; _i++) {
    var k = ks_6[_i];
    var attr = attrs[k];
    attr.name = k;
    if (attr.key) {
      pks.push(attr);
    }
  }
  for (var _a = 0, objs_2 = objs; _a < objs_2.length; _a++) {
    var obj = objs_2[_a];
    var smt = buildToSave(obj, table, attrs, ver, buildParam, pks);
    if (smt) {
      sts.push(smt);
    }
  }
  return sts;
}
exports.buildToSaveBatch = buildToSaveBatch;
var n = 'NaN';
function toString(v) {
  var x = '' + v;
  if (x === n) {
    x = 'null';
  }
  return x;
}
exports.toString = toString;
