import { Attribute, Attributes, Statement, StringMap } from "./metadata"

export function param(i: number): string {
  return ":" + i
}

export function params(length: number, from?: number): string[] {
  if (from == null) {
    from = 0
  }
  const ps: string[] = []
  for (let i = 1; i <= length; i++) {
    ps.push(param(i + from))
  }
  return ps
}

export interface OraMetadata {
  keys: Attribute[]
  bools?: Attribute[]
  map?: StringMap
  version?: string
  fields?: string[]
}

export function metadata(attrs: Attributes): OraMetadata {
  const mp: StringMap = {}
  const ks = Object.keys(attrs)
  const ats: Attribute[] = []
  const bools: Attribute[] = []
  const fields: string[] = []
  const m: OraMetadata = { keys: ats, fields }
  let isMap = false

  for (const k of ks) {
    const attr = attrs[k]
    attr.name = k
    if (attr.key) {
      ats.push(attr)
    }
    if (!attr.ignored) {
      fields.push(k)
    }
    if (attr.type === "boolean") {
      bools.push(attr)
    }
    if (attr.version) {
      m.version = k
    }
    const field = attr.column ? attr.column : k
    const s = field.toLowerCase()
    if (s !== k) {
      mp[s] = k
      isMap = true
    }
  }
  if (isMap) {
    m.map = mp
  }
  if (bools.length > 0) {
    m.bools = bools
  }
  return m
}

export function buildToInsertBatch<T>(objs: T[], table: string, attrs: Attributes, ver?: string, notSkipInvalid?: boolean, buildParam?: (i: number) => string): Statement {
  if (!buildParam) {
    buildParam = param
  }

  let i = 1
  const ks = Object.keys(attrs)
  const args: any[] = []
  const rows: string[] = []

  for (const obj of objs) {
    const cols: string[] = []
    const values: string[] = []
    let isVersion = false

    for (const k of ks) {
      let v = (obj as any)[k]
      const attr = attrs[k]

      if (attr && !attr.ignored && !attr.noinsert) {
        if (v == null) {
          v = attr.default
        }

        if (v != null) {
          const field = attr.column ? attr.column : k
          cols.push(field)

          if (k === ver) {
            isVersion = true
            values.push(`${1}`)
          } else {
            if (v === "") {
              values.push(`''`)
            } else if (typeof v === "number") {
              values.push(toString(v))
            } else if (typeof v === "boolean") {
              if (v === true) {
                const v2 = attr.true !== undefined ? attr.true : `1`
                args.push(v2)
              } else {
                const v2 = attr.false !== undefined ? attr.false : `0`
                args.push(v2)
              }
            } else {
              const p = buildParam(i++)
              values.push(p)
              args.push(v)
            }
          }
        }
      }
    }

    if (!isVersion && ver && ver.length > 0) {
      const attr = attrs[ver]
      if (attr) {
        const field = attr.column ? attr.column : ver
        cols.push(field)
        values.push(`${1}`)
      }
    }

    if (cols.length === 0) {
      if (notSkipInvalid) {
        return { query: "", params: args }
      }
    } else {
      const s = `into ${table}(${cols.join(",")})values(${values.join(",")})`
      rows.push(s)
    }
  }

  if (rows.length === 0) {
    return { query: "", params: args }
  }

  const query = `insert all ${rows.join(" ")} select * from dual`
  return { query, params: args }
}

export function buildToSave<T>(obj: T, table: string, attrs: Attributes, buildParam?: (i: number) => string, ver?: string | null, pks?: Attribute[], i?: number): Statement {
  if (i == null) {
    i = 1
  }

  if (!buildParam) {
    buildParam = param
  }

  const cols: string[] = []
  const values: string[] = []
  const args: any[] = []
  const ks = Object.keys(attrs)

  if (!pks) {
    pks = []
    for (const k of ks) {
      const attr = attrs[k]
      attr.name = k

      if (attr.key) {
        pks.push(attr)
      }
      if (attr.version) {
        ver = k
      }
    }
  }

  const colQuery: string[] = []
  const colSet: string[] = []
  let isUpdate = true

  for (const k of pks) {
    if (k.name) {
      let v = (obj as any)[k.name]
      if (v == null) {
        isUpdate = false
      }
    }
  }
  if (pks.length > 0 && isUpdate) {
    for (const pk of pks) {
      if (pk.name) {
        const attr = attrs[pk.name]
        let v = (obj as any)[pk.name]
        if (v == null) {
          v = attr.default
        }

        const field = attr.column ? attr.column : pk.name
        let x: string

        if (v === "") {
          x = `''`
        } else if (typeof v === "number") {
          x = toString(v)
        } else {
          x = buildParam(i++)
          if (typeof v === "boolean") {
            if (v === true) {
              const v2 = attr.true !== undefined ? attr.true : `1`
              args.push(v2)
            } else {
              const v2 = attr.false !== undefined ? attr.false : `0`
              args.push(v2)
            }
          } else {
            args.push(v)
          }
        }

        colQuery.push(`${field}=${x}`)
      }
    }

    if (isUpdate) {
      for (const k of ks) {
        const v = (obj as any)[k]
        if (v !== undefined) {
          const attr = attrs[k]
          if (attr && !attr.key && !attr.ignored && !attr.noupdate) {
            const field = attr.column ? attr.column : k
            let x: string
            if (attr.version) {
              ver = k
              x = `${field} + 1`
            } else {
              if (v === null) {
                x = "null"
              } else if (v === "") {
                x = `''`
              } else if (typeof v === "number") {
                x = toString(v)
              } else {
                x = buildParam(i++)
                if (typeof v === "boolean") {
                  if (v === true) {
                    const v2 = attr.true !== undefined ? attr.true : `1`
                    args.push(v2)
                  } else {
                    const v2 = attr.false !== undefined ? attr.false : `0`
                    args.push(v2)
                  }
                } else {
                  args.push(v)
                }
              }
            }
            colSet.push(`${field}=${x}`)
          }
        }
      }
    }
  }

  for (const k of ks) {
    const attr = attrs[k]
    let v = (obj as any)[k]

    if (v == null) {
      v = attr.default
    }

    if (v != null && !attr.ignored && !attr.noinsert) {
      const field = attr.column ? attr.column : k
      cols.push(field)

      if (attr.version) {
        ver = k
        values.push(`${1}`)
      } else {
        if (v === null) {
          values.push(`null`)
        } else if (v === "") {
          values.push(`''`)
        } else if (typeof v === "number") {
          values.push(toString(v))
        } else {
          const p = buildParam(i++)
          values.push(p)
          if (typeof v === "boolean") {
            if (v === true) {
              const v2 = attr.true !== undefined ? attr.true : `1`
              args.push(v2)
            } else {
              const v2 = attr.false !== undefined ? attr.false : `0`
              args.push(v2)
            }
          } else {
            args.push(v)
          }
        }
      }
    }
  }

  if (isUpdate === false || pks.length === 0) {
    if (cols.length === 0) {
      return { query: "", params: args }
    } else {
      if (pks.length === 0) {
        const q = `insert into ${table}(${cols.join(",")})values(${values.join(",")})`
        return { query: q, params: args }
      } else {
        const query = `merge into ${table} using dual on (${colQuery.join(" and ")})
  when not matched then insert (${cols.join(",")})
  values (${values.join(",")})`
        return { query, params: args }
      }
    }
  }

  const query = `merge into ${table} using dual on (${colQuery.join(" and ")})
  when matched then update set ${colSet.join(",")}
  when not matched then insert (${cols.join(",")})
      values (${values.join(",")})`
  return { query, params: args }
}

export function buildToSaveBatch<T>(objs: T[], table: string, attrs: Attributes, buildParam?: (i: number) => string): Statement[] {
  if (!buildParam) {
    buildParam = param
  }

  const sts: Statement[] = []
  const ks = Object.keys(attrs)
  let pks: Attribute[] = []
  let ver: string | null = null
  if (!pks) {
    pks = []
    for (const k of ks) {
      const attr = attrs[k]
      attr.name = k

      if (attr.key) {
        pks.push(attr)
      }
      if (attr.version) {
        ver = k
      }
    }
  }

  for (const obj of objs) {
    const smt = buildToSave(obj, table, attrs, buildParam, ver, pks)

    if (smt.query) {
      sts.push(smt)
    }
  }

  return sts
}
export function toString(v: number): string {
  if (v === v && v !== Infinity && v !== -Infinity) {
    return "" + v
  }
  return "null"
}
