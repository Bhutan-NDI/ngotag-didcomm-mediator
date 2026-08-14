import { register } from 'node:module'

// Register OpenTelemetry's ESM interception hook before the SDK and application
// modules load. Node's register() API avoids the deprecated --experimental-loader
// CLI flag while still allowing pg, ioredis and AWS SDK imports to be patched.
register('@opentelemetry/instrumentation/hook.mjs', import.meta.url)
