/**
 * dsh-ttft-hedge — TTFT 对冲插件（toolkit 形态）。
 *
 * 拦截发往 LLM 中继的 POST /v1/messages 流式请求：deadline 内未拿到响应头则
 * 并行发第二个同体请求（对冲），先 ok 先用、慢者即弃；5xx/网络错误立即重抽；
 * 4xx/429 原样返回交 dsh-llm-retry 按语义处理。内核见 ./hedge.ts（可独立单测）。
 *
 * 注入位置：host 进程 globalThis.fetch（Anthropic SDK 的 getDefaultFetch 在
 * 每次请求时读全局 fetch，pi-ai 每请求新建 client，故 patch 即时生效、无需重启）。
 * 卸载/热重载：ctx.effect 恢复原始 fetch（仅当全局 fetch 仍是本插件的包装时）。
 */
import type { Context } from '@deepseek-ai/cordis'
import { defineTool } from '@deepseek-ai/dsh-tools'
import z from '@deepseek-ai/schemastery'
import { createHedgedFetch } from './hedge.js'

export const name = 'dsh-ttft-hedge'
export const inject = ['tools']

export interface Config {
  enabled: boolean
  urlIncludes: string[]
  deadlineMs: number
  per10kTokensMs: number
  maxExtraMs: number
  maxAttempts: number
  hedgesPerMinute: number
  statsPath: string
}

export const Config = z.object({
  enabled: z.boolean().default(true),
  /** URL 含任一子串才拦截；默认覆盖所有 Anthropic Messages 端点（中继换域名也生效） */
  urlIncludes: z.array(z.string()).default(['/v1/messages']),
  /** 基础 deadline：超过仍未收到响应头 → 发对冲请求 */
  deadlineMs: z.number().default(12000),
  /** 每 1 万估算 token（body 字符/4）追加的 deadline，保护大 prefix 的合法 prefill */
  per10kTokensMs: z.number().default(1200),
  maxExtraMs: z.number().default(20000),
  maxAttempts: z.number().default(2),
  hedgesPerMinute: z.number().default(20),
  statsPath: z.string().default('/tmp/dsh-ttft-hedge/stats.json'),
})

const MARKER = Symbol.for('dsh-ttft-hedge')
const RETRIABLE = [500, 502, 503, 504, 520, 521, 522, 524]

export function apply(ctx: Context, config: Config): void {
  const origFetch = globalThis.fetch
  if (typeof origFetch !== 'function') {
    ctx.logger.warn('[ttft-hedge] globalThis.fetch 不可用，跳过安装')
    return
  }
  const prev = (origFetch as unknown as Record<symbol, unknown>)[MARKER]
  if (prev) {
    // 审计修复#4：热重载顺序颠倒时旧包装仍在——叠加会导致双层对冲（最多 maxAttempts² 发请求）。
    // 拒绝安装：旧实例继续服务，日志报错提示先卸载旧实例。
    ctx.logger.error('[ttft-hedge] globalThis.fetch 已被本插件旧实例包装，拒绝叠加安装；请先卸载旧实例（dev_uninject_plugin）后重试')
    return
  }

  const handle = createHedgedFetch(
    origFetch,
    { ...config, methods: ['POST'], retriableStatuses: RETRIABLE, statsPath: config.statsPath || undefined },
    { log: (m) => ctx.logger.info('[ttft-hedge] ' + m) },
  )
  const patched = handle.fetch as typeof globalThis.fetch & Record<symbol, unknown>
  patched[MARKER] = { installedAt: handle.stats.installedAt }
  globalThis.fetch = patched

  ctx.effect(() => () => {
    if (globalThis.fetch === patched) {
      globalThis.fetch = origFetch
      ctx.logger.info('[ttft-hedge] 已恢复原始 fetch')
    } else {
      ctx.logger.warn('[ttft-hedge] 全局 fetch 已被其他包装替换，保留现状不回滚')
    }
  }, 'dsh-ttft-hedge: restore global fetch')

  // 统计/开关工具（schema 极简，控制工具目录字符成本）
  ctx.effect(() => ctx.tools.register(defineTool({
    name: 'ttft_hedge_stats',
    description: 'TTFT 对冲统计与开关。action: stats(默认)|enable|disable',
    parameters: {
      action: { type: 'string', description: 'stats|enable|disable，缺省 stats' },
    },
    output: {
      schema: { type: 'string' },
      render: (_args: unknown, value: unknown) => [{ type: 'text', text: String(value) }],
    },
    async execute(args: { action?: string }) {
      const a = args?.action ?? 'stats'
      if (a === 'enable') { handle.config.enabled = true; return 'ttft-hedge enabled' }
      if (a === 'disable') { handle.config.enabled = false; return 'ttft-hedge disabled (passthrough)' }
      const s = handle.stats
      const ms = s.ttftLast100.map((x) => x.ms).sort((p, q) => p - q)
      const summary = {
        installedAt: s.installedAt,
        enabled: handle.config.enabled,
        intercepted: s.intercepted,
        passthrough: s.passthrough,
        hedged: s.hedged,
        hedgeWins: s.hedgeWins,
        redraws: s.redraws,
        rateLimited: s.rateLimited,
        commits: s.commits,
        failures: s.failures,
        internalErrors: s.internalErrors,
        inFlight: handle.inFlight(),
        ttftMs: ms.length ? { p50: ms[Math.floor(ms.length / 2)], p90: ms[Math.floor(ms.length * 0.9)], max: ms[ms.length - 1], n: ms.length } : null,
        recent: s.recent.slice(-12),
      }
      return JSON.stringify(summary, null, 1)
    },
  })), 'dsh-ttft-hedge: stats tool')

  ctx.logger.info('[ttft-hedge] 已安装：deadline=' + config.deadlineMs + 'ms urlIncludes=' + JSON.stringify(config.urlIncludes))
}
