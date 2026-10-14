import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-proto'
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-proto'
import { AwsInstrumentation } from '@opentelemetry/instrumentation-aws-sdk'
import { ExpressInstrumentation } from '@opentelemetry/instrumentation-express'
import { HttpInstrumentation } from '@opentelemetry/instrumentation-http'
import { IORedisInstrumentation } from '@opentelemetry/instrumentation-ioredis'
import { PgInstrumentation } from '@opentelemetry/instrumentation-pg'
import { RuntimeNodeInstrumentation } from '@opentelemetry/instrumentation-runtime-node'
import { UndiciInstrumentation } from '@opentelemetry/instrumentation-undici'
import { defaultResource, resourceFromAttributes } from '@opentelemetry/resources'
import { PeriodicExportingMetricReader } from '@opentelemetry/sdk-metrics'
import { NodeSDK } from '@opentelemetry/sdk-node'
import {
  ATTR_DEPLOYMENT_ENVIRONMENT_NAME,
  ATTR_SERVICE_NAME,
  ATTR_SERVICE_VERSION,
  ATTR_URL_FULL,
  ATTR_URL_QUERY,
} from '@opentelemetry/semantic-conventions'

const sdkEnabled = process.env.OTEL_ENABLED?.toLowerCase() === 'true'

let sdk: NodeSDK | undefined
let shutdownPromise: Promise<void> | undefined

function positiveInteger(value: string | undefined, fallback: number): number {
  if (value === undefined) return fallback
  const parsed = Number.parseInt(value, 10)
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback
}

if (sdkEnabled) {
  const resource = defaultResource().merge(
    resourceFromAttributes({
      [ATTR_SERVICE_NAME]: process.env.OTEL_SERVICE_NAME ?? 'didcomm-mediator',
      ...(process.env.SERVICE_VERSION ? { [ATTR_SERVICE_VERSION]: process.env.SERVICE_VERSION } : {}),
      ...(process.env.DEPLOYMENT_ENVIRONMENT
        ? { [ATTR_DEPLOYMENT_ENVIRONMENT_NAME]: process.env.DEPLOYMENT_ENVIRONMENT }
        : {}),
    })
  )

  sdk = new NodeSDK({
    resource,
    traceExporter: new OTLPTraceExporter(),
    metricReaders: [
      new PeriodicExportingMetricReader({
        exporter: new OTLPMetricExporter(),
        exportIntervalMillis: positiveInteger(process.env.OTEL_METRIC_EXPORT_INTERVAL, 60_000),
      }),
    ],
    instrumentations: [
      new HttpInstrumentation({
        ignoreIncomingRequestHook: (request) => request.url?.split('?', 1)[0] === '/health',
        redactedQueryParams: ['_oobid', 'token', 'access_token', 'api_key'],
      }),
      new ExpressInstrumentation(),
      new UndiciInstrumentation({
        startSpanHook: (request) => {
          try {
            const url = new URL(request.path, request.origin)
            url.search = ''
            return { [ATTR_URL_FULL]: url.toString(), [ATTR_URL_QUERY]: '' }
          } catch {
            return { [ATTR_URL_QUERY]: '' }
          }
        },
      }),
      new PgInstrumentation({ enhancedDatabaseReporting: false }),
      new IORedisInstrumentation(),
      new AwsInstrumentation({ suppressInternalInstrumentation: true }),
      new RuntimeNodeInstrumentation({ monitoringPrecision: 5_000 }),
    ],
  })

  sdk.start()
}

export function isTelemetrySdkEnabled(): boolean {
  return sdk !== undefined
}

export function shutdownTelemetry(): Promise<void> {
  if (!sdk) return Promise.resolve()
  shutdownPromise ??= sdk.shutdown()
  return shutdownPromise
}
