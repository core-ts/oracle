import { Connection } from 'oracledb';

export interface AnyMap {
  [key: string]: any;
}
export interface HealthChecker {
  name(): string;
  build(data: AnyMap, error: any): AnyMap;
  check(): Promise<AnyMap>;
}

export class OracleChecker implements HealthChecker {
  timeout: number;
  service: string;
  constructor(private client: Connection, service?: string, timeout?: number) {
    this.timeout = (timeout ? timeout : 4200);
    this.service = (service ? service : 'oracle');
    this.check = this.check.bind(this);
    this.name = this.name.bind(this);
    this.build = this.build.bind(this);
  }
  check(): Promise<AnyMap> {
    const obj = {} as AnyMap;
    const promise = new Promise<any>((resolve, reject) => {
      this.client.ping((err) => {
        if (err) {
          return reject(err);
        } else {
          resolve(obj);
        }
      });
    });
    if (this.timeout > 0) {
      return promiseTimeOut(this.timeout, promise);
    } else {
      return promise;
    }
  }
  name(): string {
    return this.service;
  }
  build(data: AnyMap, err: any): AnyMap {
    if (err) {
      if (!data) {
        data = {} as AnyMap;
      }
      data['error'] = err;
    }
    return data;
  }
}

function promiseTimeOut(timeoutInMilliseconds: number, promise: Promise<any>): Promise<any> {
  return Promise.race([
    promise,
    new Promise((resolve, reject) => {
      setTimeout(() => {
        reject(`Timed out in: ${timeoutInMilliseconds} milliseconds!`);
      }, timeoutInMilliseconds);
    })
  ]);
}
