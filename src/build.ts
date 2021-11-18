import { Attribute, Attributes, Statement, StringMap } from './metadata';

export function param(i: number): string {
  return ':' + i;
}
export function params(length: number, from?: number): string[] {
  if (from === undefined || from == null) {
    from = 0;
  }
  const ps: string[] = [];
  for (let i = 1; i <= length; i++) {
    ps.push(param(i + from));
  }
  return ps;
}
export interface Metadata {
  keys: Attribute[];
  bools?: Attribute[];
  map?: StringMap;
  version?: string;
  fields?: string[];
}
export function metadata(attrs: Attributes): Metadata {
  const mp: StringMap = {};
  const ks = Object.keys(attrs);
  const ats: Attribute[] = [];
  const bools: Attribute[] = [];
  const fields: string[] = [];
  const m: Metadata = {keys: ats, fields};
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
    if (attr.type === 'boolean') {
      bools.push(attr);
    }
    if (attr.version) {
      m.version = k;
    }
    const field = (attr.field ? attr.field : k);
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
export function buildToInsertBatch<T>(objs: T[], table: string, attrs: Attributes, ver?: string, notSkipInvalid?: boolean, buildParam?: (i: number) => string): Statement|undefined {
  if (!buildParam) {
    buildParam = param;
  }
  let i = 1;
  const ks = Object.keys(attrs);
  const args: any[] = [];
  const rows: string[] = [];
  for (const obj of objs) {
    const cols: string[] = [];
    const values: string[] = [];
    let isVersion = false;
    for (const k of ks) {
      let v = (obj as any)[k];
      const attr = attrs[k];
      if (attr && !attr.ignored && !attr.noinsert) {
        if (v === undefined || v == null) {
          v = attr.default;
        }
        if (v !== undefined && v != null) {
          const field = (attr.field ? attr.field : k);
          cols.push(field);
          if (k === ver) {
            isVersion = true;
            values.push(`${1}`);
          } else {
            if (v === '') {
              values.push(`''`);
            } else if (typeof v === 'number') {
              values.push(toString(v));
            } else if (typeof v === 'boolean') {
              if (attr.true === undefined) {
                if (v === true) {
                  values.push(`true`);
                } else {
                  values.push(`false`);
                }
              } else {
                const p = buildParam(i++);
                values.push(p);
                if (v === true) {
                  const v2 = (attr.true ? attr.true : '1');
                  args.push(v2);
                } else {
                  const v2 = (attr.false ? attr.false : '0');
                  args.push(v2);
                }
              }
            } else {
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
      const field = (attr.field ? attr.field : ver);
      cols.push(field);
      values.push(`${1}`);
    }
    if (cols.length === 0) {
      if (notSkipInvalid) {
        return undefined;
      }
    } else {
      const s = `into ${table}(${cols.join(',')})values(${values.join(',')})`;
      rows.push(s);
    }
  }
  if (rows.length === 0) {
    return undefined;
  }
  const query = `insert all ${rows.join(' ')} select * from dual`;
  return { query, params: args };
}
export function buildToSave<T>(obj: T, table: string, attrs: Attributes, ver?: string, buildParam?: (i: number) => string, pks?: Attribute[], i?: number): Statement|undefined {
  if (!i) {
    i = 1;
  }
  if (!buildParam) {
    buildParam = param;
  }
  const cols: string[] = [];
  const values: string[] = [];
  const args: any[] = [];
  let isVersion = false;
  const ks = Object.keys(attrs);
  if (!pks) {
    pks = [];
    for (const k of ks) {
      const attr = attrs[k];
      attr.name = k;
      if (attr.key) {
        pks.push(attr);
      }
    }
  }
  const colQuery: string[] = [];
  const colSet: string[] = [];
  let noUpdate = false;
  if (pks.length > 0) {
    for (const pk of pks) {
      if (pk.name) {
        const v = (obj as any)[pk.name];
        if (!v) {
          return undefined;
        } else {
          const attr = attrs[pk.name];
          const field = (attr.field ? attr.field : pk.name);
          let x: string;
          if (v == null) {
            x = 'null';
            noUpdate = true;
          } else if (v === '') {
            x = `''`;
          } else if (typeof v === 'number') {
            x = toString(v);
          } else {
            x = buildParam(i++);
            if (typeof v === 'boolean') {
              if (v === true) {
                const v2 = (attr.true ? '' + attr.true : `'1'`);
                args.push(v2);
              } else {
                const v2 = (attr.false ? '' + attr.false : `'0'`);
                args.push(v2);
              }
            } else {
              args.push(v);
            }
          }
          colQuery.push(`${field}=${x}`);
        }
      }
    }
    for (const k of ks) {
      const v = (obj as any)[k];
      if (v !== undefined) {
        const attr = attrs[k];
        if (!attr.key && !attr.ignored && k !== ver && !attr.noupdate) {
          const field = (attr.field ? attr.field : k);
          let x: string;
          if (v == null) {
            x = 'null';
          } else if (v === '') {
            x = `''`;
          } else if (typeof v === 'number') {
            x = toString(v);
          } else if (typeof v === 'boolean') {
            if (attr.true === undefined) {
              if (v === true) {
                x = `'1'`;
              } else {
                x = `'0'`;
              }
            } else {
              x = buildParam(i++);
              if (v === true) {
                const v2 = (attr.true ? attr.true : `'1'`);
                args.push(v2);
              } else {
                const v2 = (attr.false ? attr.false : `'0'`);
                args.push(v2);
              }
            }
          } else {
            x = buildParam(i++);
            args.push(v);
          }
          colSet.push(`${field}=${x}`);
        }
      }
    }
  }
  for (const k of ks) {
    const attr = attrs[k];
    let v = (obj as any)[k];
    if (v === undefined || v == null) {
      v = attr.default;
    }
    if (v !== undefined && v != null && !attr.ignored && !attr.noinsert) {
      const field = (attr.field ? attr.field : k);
      cols.push(field);
      if (k === ver) {
        isVersion = true;
        values.push(`${1}`);
      } else {
        if (v === '') {
          values.push(`''`);
        } else if (typeof v === 'number') {
          values.push(toString(v));
        } else if (typeof v === 'boolean') {
          if (attr.true === undefined) {
            if (v === true) {
              values.push(`true`);
            } else {
              values.push(`false`);
            }
          } else {
            const p = buildParam(i++);
            values.push(p);
            if (v === true) {
              const v2 = (attr.true ? attr.true : `'1'`);
              args.push(v2);
            } else {
              const v2 = (attr.false ? attr.false : `'0'`);
              args.push(v2);
            }
          }
        } else {
          const p = buildParam(i++);
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
    const attr = attrs[ver];
    const field = (attr.field ? attr.field : ver);
    cols.push(field);
    values.push(`${1}`);
  }
  if (noUpdate || pks.length === 0 || colSet.length === 0) {
    const q = `insert into ${table}(${cols.join(',')})values(${values.join(',')})`;
    return { query: q, params: args };
  } else {
    if (ver && ver.length > 0) {
      const v = (obj as any)[ver];
      if (typeof v === 'number' && !isNaN(v)) {
        const attr = attrs[ver];
        if (attr) {
          const field = (attr.field ? attr.field : ver);
          colSet.push(`${field}=${(1 + v)}`);
          colQuery.push(`${field}=${v}`);
        }
      }
    }
    const query = `merge into ${table} using dual on (${colQuery.join(' and ')})
    when matched then update set ${colSet.join(',')}
    when not matched then insert (${cols.join(',')})
        values (${values.join(',')})
`;
    return { query, params: args };
  }
}
export function buildToSaveBatch<T>(objs: T[], table: string, attrs: Attributes, ver?: string, buildParam?: (i: number) => string): Statement[] {
  if (!buildParam) {
    buildParam = param;
  }
  const sts: Statement[] = [];
  const ks = Object.keys(attrs);
  const pks: Attribute[] = [];
  for (const k of ks) {
    const attr = attrs[k];
    attr.name = k;
    if (attr.key) {
      pks.push(attr);
    }
  }
  for (const obj of objs) {
    const smt = buildToSave(obj, table, attrs, ver, buildParam, pks);
    if (smt) {
      sts.push(smt);
    }
  }
  return sts;
}
const n = 'NaN';
export function toString(v: number): string {
  let x = '' + v;
  if (x === n) {
    x = 'null';
  }
  return x;
}
