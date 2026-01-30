import type { StreamFn } from "@mariozechner/pi-agent-core";
import type { Api, Model, SimpleStreamOptions, ThinkingBudgets } from "@mariozechner/pi-ai";
import { streamSimple } from "@mariozechner/pi-ai";

import type { OpenClawConfig } from "../../config/config.js";
import { log } from "./logger.js";

/**
 * Resolve provider-specific extra params from model config.
 * Used to pass through stream params like temperature/maxTokens.
 *
 * @internal Exported for testing only
 */
export function resolveExtraParams(params: {
  cfg: OpenClawConfig | undefined;
  provider: string;
  modelId: string;
}): Record<string, unknown> | undefined {
  const modelKey = `${params.provider}/${params.modelId}`;
  const modelConfig = params.cfg?.agents?.defaults?.models?.[modelKey];
  return modelConfig?.params ? { ...modelConfig.params } : undefined;
}

type CacheControlTtl = "5m" | "1h";
type ThinkingLevel = "minimal" | "low" | "medium" | "high" | "xhigh";
type PayloadAuditLevel = "debug" | "info";

const THINKING_LEVELS = new Set<ThinkingLevel>(["minimal", "low", "medium", "high", "xhigh"]);

function normalizeThinkingLevel(raw: unknown): ThinkingLevel | undefined {
  if (typeof raw !== "string") return undefined;
  const normalized = raw.trim().toLowerCase();
  if (!normalized) return undefined;
  return THINKING_LEVELS.has(normalized as ThinkingLevel)
    ? (normalized as ThinkingLevel)
    : undefined;
}

function resolveThinkingBudgets(raw: unknown): ThinkingBudgets | undefined {
  if (!raw || typeof raw !== "object") return undefined;
  const source = raw as Record<string, unknown>;
  const budgets: ThinkingBudgets = {};
  if (typeof source.minimal === "number") budgets.minimal = source.minimal;
  if (typeof source.low === "number") budgets.low = source.low;
  if (typeof source.medium === "number") budgets.medium = source.medium;
  if (typeof source.high === "number") budgets.high = source.high;
  return Object.keys(budgets).length > 0 ? budgets : undefined;
}

function resolveHeaders(raw: unknown): Record<string, string> | undefined {
  if (!raw || typeof raw !== "object") return undefined;
  const headers: Record<string, string> = {};
  for (const [key, value] of Object.entries(raw as Record<string, unknown>)) {
    if (typeof value === "string") {
      headers[key] = value;
    }
  }
  return Object.keys(headers).length > 0 ? headers : undefined;
}

function resolvePayloadAudit(raw: unknown): PayloadAuditLevel | undefined {
  if (raw === true) return "debug";
  if (raw === "debug" || raw === "info") return raw;
  return undefined;
}

function resolvePayloadOverrides(
  extraParams: Record<string, unknown> | undefined,
): Record<string, unknown> | undefined {
  if (!extraParams) return undefined;
  const overrides: Record<string, unknown> = {};
  const payloadOverrides = extraParams.payloadOverrides ?? extraParams.payloadOverride;
  if (payloadOverrides && typeof payloadOverrides === "object") {
    Object.assign(overrides, payloadOverrides as Record<string, unknown>);
  }
  if (typeof extraParams.partial === "boolean") {
    overrides.partial = extraParams.partial;
  }
  return Object.keys(overrides).length > 0 ? overrides : undefined;
}

function resolveCacheControlTtl(
  extraParams: Record<string, unknown> | undefined,
  provider: string,
  modelId: string,
): CacheControlTtl | undefined {
  const raw = extraParams?.cacheControlTtl;
  if (raw !== "5m" && raw !== "1h") return undefined;
  if (provider === "anthropic") return raw;
  if (provider === "openrouter" && modelId.startsWith("anthropic/")) return raw;
  return undefined;
}

function mergeHeaders(
  base: Record<string, string> | undefined,
  override: Record<string, string> | undefined,
): Record<string, string> | undefined {
  if (!base && !override) return undefined;
  return { ...(base ?? {}), ...(override ?? {}) };
}

function mergePayloadHandlers(
  base: ((payload: unknown) => void) | undefined,
  override: ((payload: unknown) => void) | undefined,
): ((payload: unknown) => void) | undefined {
  if (!base && !override) return undefined;
  if (base && override) {
    return (payload) => {
      base(payload);
      override(payload);
    };
  }
  return base ?? override;
}

function createStreamFnWithExtraParams(
  baseStreamFn: StreamFn | undefined,
  extraParams: Record<string, unknown> | undefined,
  provider: string,
  modelId: string,
): StreamFn | undefined {
  if (!extraParams || Object.keys(extraParams).length === 0) {
    return undefined;
  }

  const streamParams: Partial<SimpleStreamOptions> & { cacheControlTtl?: CacheControlTtl } = {};
  if (typeof extraParams.temperature === "number") {
    streamParams.temperature = extraParams.temperature;
  }
  if (typeof extraParams.maxTokens === "number") {
    streamParams.maxTokens = extraParams.maxTokens;
  }
  const cacheControlTtl = resolveCacheControlTtl(extraParams, provider, modelId);
  if (cacheControlTtl) {
    streamParams.cacheControlTtl = cacheControlTtl;
  }
  const reasoning = normalizeThinkingLevel(extraParams.reasoning ?? extraParams.thinking);
  if (reasoning) {
    streamParams.reasoning = reasoning;
  }
  const thinkingBudgets = resolveThinkingBudgets(extraParams.thinkingBudgets);
  if (thinkingBudgets) {
    streamParams.thinkingBudgets = thinkingBudgets;
  }
  if (typeof extraParams.sessionId === "string" && extraParams.sessionId.trim()) {
    streamParams.sessionId = extraParams.sessionId.trim();
  }
  const headers = resolveHeaders(extraParams.headers);
  if (headers) {
    streamParams.headers = headers;
  }

  const payloadOverrides = resolvePayloadOverrides(extraParams);
  const payloadAudit = resolvePayloadAudit(extraParams.payloadAudit);
  if (payloadOverrides || payloadAudit) {
    streamParams.onPayload = (payload) => {
      if (payloadOverrides && payload && typeof payload === "object") {
        Object.assign(payload as Record<string, unknown>, payloadOverrides);
      }
      if (payloadAudit) {
        const logger = payloadAudit === "info" ? log.info.bind(log) : log.debug.bind(log);
        logger(`payload audit (${provider}/${modelId}): ${JSON.stringify(payload)}`);
      }
    };
  }

  if (Object.keys(streamParams).length === 0) {
    return undefined;
  }

  log.debug(`creating streamFn wrapper with params: ${JSON.stringify(streamParams)}`);

  const underlying = baseStreamFn ?? streamSimple;
  const wrappedStreamFn: StreamFn = (model, context, options) => {
    const { headers: baseHeaders, onPayload: baseOnPayload, ...baseParams } = streamParams;
    const mergedHeaders = mergeHeaders(baseHeaders, options?.headers);
    const mergedOnPayload = mergePayloadHandlers(baseOnPayload, options?.onPayload);

    return underlying(model as Model<Api>, context, {
      ...baseParams,
      ...options,
      headers: mergedHeaders,
      onPayload: mergedOnPayload,
    });
  };

  return wrappedStreamFn;
}

/**
 * Apply extra params (like temperature) to an agent's streamFn.
 *
 * @internal Exported for testing
 */
export function applyExtraParamsToAgent(
  agent: { streamFn?: StreamFn },
  cfg: OpenClawConfig | undefined,
  provider: string,
  modelId: string,
  extraParamsOverride?: Record<string, unknown>,
): void {
  const extraParams = resolveExtraParams({
    cfg,
    provider,
    modelId,
  });
  const override =
    extraParamsOverride && Object.keys(extraParamsOverride).length > 0
      ? Object.fromEntries(
          Object.entries(extraParamsOverride).filter(([, value]) => value !== undefined),
        )
      : undefined;
  const merged = Object.assign({}, extraParams, override);
  const wrappedStreamFn = createStreamFnWithExtraParams(agent.streamFn, merged, provider, modelId);

  if (wrappedStreamFn) {
    log.debug(`applying extraParams to agent streamFn for ${provider}/${modelId}`);
    agent.streamFn = wrappedStreamFn;
  }
}
