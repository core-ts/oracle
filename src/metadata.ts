export interface StringMap {
  [key: string]: string
}

export interface Statement {
  query: string
  params?: any[]
}

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
export interface Transaction extends Executor {
  commit(): Promise<void>
  rollback(): Promise<void>
}
export interface DB extends Executor {
  beginTransaction(): Promise<Transaction>
}

export type DataType = "ObjectId" | "date" | "datetime" | "time" | "boolean" | "number" | "integer" | "string" | "text" | "object" | "array" | "binary" | "primitives" | "booleans" | "numbers" | "integers" | "strings" | "dates" | "datetimes" | "times"

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

export interface Attributes {
  [key: string]: Attribute
}
