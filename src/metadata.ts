export interface StringMap {
  [key: string]: string;
}
export interface Statement {
  query: string;
  params?: any[];
}

export interface Manager {
  driver: string;
  param(i: number): string;
  exec(sql: string, args?: any[], ctx?: any): Promise<number>;
  execBatch(statements: Statement[], firstSuccess?: boolean, ctx?: any): Promise<number>;
  query<T>(sql: string, args?: any[], m?: StringMap, bools?: Attribute[], ctx?: any): Promise<T[]>;
  queryOne<T>(sql: string, args?: any[], m?: StringMap, bools?: Attribute[], ctx?: any): Promise<T|null>;
  execScalar<T>(sql: string, args?: any[], ctx?: any): Promise<T>;
  count(sql: string, args?: any[], ctx?: any): Promise<number>;
}
export interface DB {
  driver: string;
  param(i: number): string;
  exec(sql: string, args?: any[], ctx?: any): Promise<number>;
  execBatch(statements: Statement[], firstSuccess?: boolean, ctx?: any): Promise<number>;
  query<T>(sql: string, args?: any[], m?: StringMap, bools?: Attribute[], ctx?: any): Promise<T[]>;
  queryOne<T>(sql: string, args?: any[], m?: StringMap, bools?: Attribute[], ctx?: any): Promise<T|null>;
  execScalar<T>(sql: string, args?: any[], ctx?: any): Promise<T>;
  count(sql: string, args?: any[], ctx?: any): Promise<number>;
}

export type DataType = 'ObjectId' | 'date' | 'datetime' | 'time'
  | 'boolean' | 'number' | 'integer' | 'string' | 'text'
  | 'object' | 'array' | 'binary'
  | 'primitives' | 'booleans' | 'numbers' | 'integers' | 'strings' | 'dates' | 'datetimes' | 'times';
export type FormatType = 'currency' | 'percentage' | 'email' | 'url' | 'phone' | 'fax' | 'ipv4' | 'ipv6';
export type MatchType = 'equal' | 'prefix' | 'contain' | 'max' | 'min'; // contain: default for string, min: default for Date, number

export interface Model {
  name?: string;
  attributes: Attributes;
  source?: string;
  table?: string;
  collection?: string;
  model?: any;
  schema?: any;
}
export interface Attribute {
  name?: string;
  field?: string;
  column?: string;
  type?: DataType;
  format?: FormatType;
  required?: boolean;
  match?: MatchType;
  default?: string|number|Date|boolean;
  key?: boolean;
  unique?: boolean;
  enum?: string[] | number[];
  q?: boolean;
  noinsert?: boolean;
  noupdate?: boolean;
  nopatch?: boolean;
  version?: boolean;
  length?: number;
  min?: number;
  max?: number;
  gt?: number;
  lt?: number;
  precision?: number;
  scale?: number;
  exp?: RegExp | string;
  code?: string;
  noformat?: boolean;
  ignored?: boolean;
  jsonField?: string;
  link?: string;
  typeof?: Attributes;
  true?: string|number;
  false?: string|number;
}
export interface Attributes {
  [key: string]: Attribute;
}
