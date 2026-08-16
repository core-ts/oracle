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
  private static readonly TIMEOUT = 4500

  constructor(
    private readonly pool: Pool,
    private readonly checkerName = "oracle",
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

      connection.callTimeout = OracleChecker.TIMEOUT

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
