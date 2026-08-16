import type { Pool } from "oracledb"

export interface AnyMap {
  [key: string]: any
}

export interface HealthChecker {
  name(): string
  build(data: AnyMap, error: any): AnyMap
  check(): Promise<AnyMap>
}

export class OracleChecker implements HealthChecker {
  constructor(
    protected readonly pool: Pool,
    protected readonly checkerName = "oracle",
    protected readonly timeout = 4500,
  ) {}

  name(): string {
    return this.checkerName
  }

  build(data: AnyMap, error: any): AnyMap {
    return {
      name: this.name(),
      status: "DOWN",
      ...data,
      error: error?.message ?? error,
    }
  }

  async check(): Promise<AnyMap> {
    let connection

    try {
      connection = await this.pool.getConnection()

      connection.callTimeout = this.timeout

      await connection.execute("SELECT 1 FROM DUAL")

      return {
        name: this.name(),
        status: "UP",
      }
    } catch (error) {
      return this.build({}, error)
    } finally {
      if (connection) {
        try {
          await connection.close()
        } catch {
          // Ignore connection release errors.
        }
      }
    }
  }
}
