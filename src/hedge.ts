/**
 * hedge.ts — TTFT 对冲 fetch 包装器（无 cordis 依赖，可独立单测）。
 *
 * 背景（cache-evidence 2026-08-22 + 2026-09-10 实测）：chat-relay 上游为多池随机路由，
 * 同一形态请求 TTFT 在 0.8s~50s 间抖动，偶发 HTTP 504。对冲（hedged request）把尾部
 * 延迟封顶：deadline 内未拿到响应头 → 并行发第二个同体请求，先 ok 先用、慢者即弃；
 * 5xx/网络错误 → 立即重抽；4xx/429 → 原样返回交上游（dsh-llm-retry）按语义处理。
 *
 * 安全边界：
 * - 仅拦截 method ∈ methods 且 URL 含 urlIncludes 子串、body 为 string/空 的请求；
 * - 内部异常 → fail-open 清理全部在途后直落原始 fetch；循环正常完结后的上游错误
 *   （finalized）原样上抛，绝不触发 fail-open 的隐藏第三发；
 * - maxAttempts 封顶（默认 2）+ 投机对冲限流（每分钟），失败重抽独立闸门不占投机预算；
 * - caller abort：在途全部中止上抛 AbortError；已 commit 的胜者保留 caller→ac 桥接，
 *   流式读 body 期间 caller 再 abort 仍能切断上游（恢复原生 fetch 语义）。
 *
 * 结算模型：outcome 队列 + notify 唤醒（对冲尝试由 setTimeout/失败重抽异步追加，
 * Promise.race 快照收不到后来者，队列天然支持动态追加）。
 *
 * 2026-09-10 审计修复（7 项）：commit 后 abort 桥接保留 / 中间坏响应 body cancel /
 * fail-open 全量清理 / 热重载叠加拒绝（index.ts）/ 对冲与重抽预算分离 /
 * timer 回调 try-catch / fireHedge abort 预检。
 */
import { mkdirSync, writeFileSync, renameSync } from 'node:fs'
import { dirname } from 'node:path'

type FetchInput = Parameters<typeof globalThis.fetch>[0]

export interface HedgeConfig {
  enabled: boolean
  urlIncludes: string[]
  methods: string[]
  deadlineMs: number
  /** 每 1 万估算 token（bodyChars/4）追加的 deadline 毫秒数（大 prefix 的合法 prefill 更久） */
  per10kTokensMs: number
  /** 追加上限 */
  maxExtraMs: number
  maxAttempts: number
  hedgesPerMinute: number
  retriableStatuses: number[]
  statsPath?: string
}

export interface HedgeEvent { t: string; reason: string; detail: string }

interface Sample { ms: number; hedged: boolean; winner: string; wire?: string }

export interface HedgeStats {
  installedAt: string
  intercepted: number
  passthrough: number
  hedged: number
  hedgeWins: number
  redraws: number
  rateLimited: number
  commits: number
  failures: number
  internalErrors: number
  ttftLast100: Sample[]
  recent: HedgeEvent[]
}

interface Outcome { res?: Response; err?: unknown }
interface Attempt {
  idx: number
  reason: string
  ac: AbortController
  startedAt: number
  settledAt?: number
  outcome?: Outcome
  p: Promise<Outcome>
  onCallerAbort?: () => void
  counted?: boolean
  unlistened?: boolean
}

function deadlineFor(bodyLen: number, cfg: HedgeConfig): number {
  const estTokens = bodyLen / 4
  const extra = Math.min(cfg.maxExtraMs, Math.floor(estTokens / 10000) * cfg.per10kTokensMs)
  return cfg.deadlineMs + extra
}

function shortErr(err: unknown): string {
  const e = err as { name?: string; message?: string } | undefined
  return (e?.name ?? 'Error') + ': ' + String(e?.message ?? err).slice(0, 120)
}

/** 从请求体正则提取线上参数（免全量 JSON.parse），用于验证配置热生效 */
function wireParams(body: unknown): string {
  if (typeof body !== 'string') return ''
  const mt = /"max_tokens":\s*(\d+)/.exec(body)
  const tb = /"budget_tokens":\s*(\d+)/.exec(body)
  const model = /"model":\s*"([^"]+)"/.exec(body)
  return (model ? 'model=' + model[1] : '') + (mt ? ' mt=' + mt[1] : '') + (tb ? ' tb=' + tb[1] : '')
}

export interface HedgedFetchHandle {
  fetch: typeof globalThis.fetch
  stats: HedgeStats
  config: HedgeConfig
  /** 供测试/诊断：在途尝试数 */
  inFlight: () => number
}

export function createHedgedFetch(
  origFetch: typeof globalThis.fetch,
  cfg: HedgeConfig,
  hooks?: { log?: (msg: string) => void },
): HedgedFetchHandle {
  const log = hooks?.log ?? (() => {})
  const stats: HedgeStats = {
    installedAt: new Date().toISOString(),
    intercepted: 0, passthrough: 0, hedged: 0, hedgeWins: 0, redraws: 0,
    rateLimited: 0, commits: 0, failures: 0, internalErrors: 0,
    ttftLast100: [], recent: [],
  }
  const hedgeTimes: number[] = []
  let inFlight = 0

  const persist = () => {
    if (!cfg.statsPath) return
    try {
      mkdirSync(dirname(cfg.statsPath), { recursive: true })
      const tmp = cfg.statsPath + '.tmp'
      writeFileSync(tmp, JSON.stringify({ ...stats, config: cfg }, null, 1))
      renameSync(tmp, cfg.statsPath) // 原子替换，外部监控不会读到半截 JSON
    } catch { /* 统计写盘失败不影响主流程 */ }
  }
  const record = (reason: string, detail: string) => {
    stats.recent.push({ t: new Date().toISOString(), reason, detail })
    if (stats.recent.length > 50) stats.recent.shift()
    persist()
  }
  const rateOk = (): boolean => {
    const now = Date.now()
    while (hedgeTimes.length && now - hedgeTimes[0] > 60000) hedgeTimes.shift()
    if (hedgeTimes.length >= cfg.hedgesPerMinute) return false
    hedgeTimes.push(now)
    return true
  }
  const noteCommit = (ms: number, hedged: boolean, winner: string, wire: string) => {
    stats.commits++
    stats.ttftLast100.push({ ms: Math.round(ms), hedged, winner, wire })
    if (stats.ttftLast100.length > 100) stats.ttftLast100.shift()
    persist()
  }

  function shouldHedge(url: string, init: RequestInit | undefined): boolean {
    if (!cfg.enabled) return false
    const method = String(init?.method ?? 'GET').toUpperCase()
    if (!cfg.methods.includes(method)) return false
    if (!cfg.urlIncludes.some((s) => url.includes(s))) return false
    const body = init?.body
    if (body != null && typeof body !== 'string') return false
    return true
  }

  const hedgedFetch: typeof globalThis.fetch = async (input, init) => {
    let url = ''
    try {
      if (typeof input === 'string') url = input
      else if (input instanceof URL) url = input.href
      else return origFetch(input as FetchInput, init) // Request 对象：透传
    } catch { return origFetch(input as FetchInput, init) }
    if (!shouldHedge(url, init)) { stats.passthrough++; return origFetch(input as FetchInput, init) }
    stats.intercepted++

    // 函数级作用域：fail-open catch 也能全量清理（审计修复#3）
    const attempts: Attempt[] = []
    let timer: ReturnType<typeof setTimeout> | null = null
    let finalized = false
    const clearTimer = () => { if (timer) { clearTimeout(timer); timer = null } }
    const decSlot = (a: Attempt) => { if (!a.counted) { a.counted = true; inFlight-- } }
    const unlisten = (a: Attempt) => {
      if (a.unlistened) return
      a.unlistened = true
      const callerSignal = (init?.signal as AbortSignal | null | undefined) ?? null
      if (a.onCallerAbort && callerSignal) { try { callerSignal.removeEventListener('abort', a.onCallerAbort) } catch { /* noop */ } }
    }
    const cancelBody = (o?: Outcome) => { if (o?.res) { try { o.res.body?.cancel().catch(() => {}) } catch { /* noop */ } } }
    const discard = (a: Attempt) => {
      decSlot(a); unlisten(a)
      try { a.ac.abort() } catch { /* noop */ }
      a.p.then((o) => cancelBody(o)).catch(() => {})
    }

    try {
      const callerSignal = (init?.signal as AbortSignal | null | undefined) ?? null
      if (callerSignal?.aborted) return origFetch(input as FetchInput, init)
      const bodyStr = typeof init?.body === 'string' ? init.body : ''
      const wire = wireParams(init?.body)
      const t0 = Date.now()
      // outcome 队列 + 唤醒（支持异步追加尝试）
      const queue: Array<{ o: Outcome; a: Attempt }> = []
      let notify: (() => void) | null = null
      const pushOutcome = (entry: { o: Outcome; a: Attempt }) => {
        queue.push(entry)
        if (notify) { const n = notify; notify = null; n() }
      }
      const nextOutcome = async (): Promise<{ o: Outcome; a: Attempt } | null> => {
        for (;;) {
          if (queue.length) return queue.shift()!
          const allSettled = attempts.length > 0 && attempts.every((x) => x.outcome !== undefined)
          if (allSettled) return null
          await new Promise<void>((r) => { notify = r })
        }
      }

      let hedgeFired = false
      const startAttempt = (reason: string): Attempt => {
        const ac = new AbortController()
        const a: Attempt = { idx: attempts.length, reason, ac, startedAt: Date.now(), p: null! }
        if (callerSignal) {
          a.onCallerAbort = () => { try { ac.abort(callerSignal.reason) } catch { /* noop */ } }
          callerSignal.addEventListener('abort', a.onCallerAbort, { once: true })
        }
        a.p = origFetch(url, { ...init, signal: ac.signal } as RequestInit)
          .then((res) => ({ res }))
          .catch((err) => ({ err }))
        a.p.then((o) => { a.settledAt = Date.now(); a.outcome = o; pushOutcome({ o, a }) }).catch(() => {})
        attempts.push(a)
        inFlight++
        return a
      }
      /** kind='deadline'：投机对冲，占限流预算，一次为限；kind='redraw'：失败驱动重抽，独立闸门（审计修复#5） */
      const fireHedge = (reason: string, kind: 'deadline' | 'redraw'): Attempt | null => {
        if (callerSignal?.aborted) return null // 审计修复#7
        if (attempts.length >= cfg.maxAttempts) return null
        if (kind === 'deadline') {
          if (hedgeFired) return null
          hedgeFired = true
          if (!rateOk()) { stats.rateLimited++; record('rate-limited', reason); return null }
        }
        clearTimer()
        if (kind === 'deadline') stats.hedged++
        record(kind === 'deadline' ? 'hedge' : 'redraw-attempt', reason + ' at ' + (Date.now() - t0) + 'ms, bodyLen=' + bodyStr.length)
        log(kind + ' fired (' + reason + ') at ' + (Date.now() - t0) + 'ms url=' + url.slice(0, 80))
        return startAttempt(reason)
      }

      const A = startAttempt('primary')
      timer = setTimeout(() => {
        try { fireHedge('deadline', 'deadline') } catch (e) { record('timer-error', shortErr(e)); log('timer error: ' + shortErr(e)) } // 审计修复#6
      }, deadlineFor(bodyStr.length, cfg))
      let lastBad: { o: Outcome; a: Attempt } | null = null

      for (;;) {
        const next = await nextOutcome()
        if (!next) break
        const { o, a } = next
        if (callerSignal?.aborted) {
          for (const x of attempts) if (x !== a) discard(x)
          clearTimer()
          if (o.err) { decSlot(a); unlisten(a); finalized = true; throw o.err }
          decSlot(a) // 返回响应：保留 caller→ac 桥接（body 随 abort 自然终止）
          finalized = true
          return o.res as Response
        }
        if (o.res && o.res.ok) {
          for (const x of attempts) if (x !== a) discard(x)
          clearTimer()
          decSlot(a) // 审计修复#1：胜者不摘 listener——流式读 body 期间 caller abort 仍能切断上游
          if (a.idx > 0) { stats.hedgeWins++; record('hedge-win', 'attempt#' + a.idx + ' (' + a.reason + ') won at ' + (Date.now() - t0) + 'ms') }
          noteCommit(Date.now() - t0, hedgeFired, a.idx === 0 ? 'primary' : a.reason, wire)
          log('commit via ' + (a.idx === 0 ? 'primary' : 'hedge(' + a.reason + ')') + ' in ' + (Date.now() - t0) + 'ms ' + wire)
          finalized = true
          return o.res
        }
        if (o.res && !cfg.retriableStatuses.includes(o.res.status)) {
          // 4xx/429：原样返回，交上游按语义处理（不重试、不对冲）
          for (const x of attempts) if (x !== a) discard(x)
          clearTimer()
          decSlot(a)
          noteCommit(Date.now() - t0, hedgeFired, 'status-' + o.res.status, wire)
          finalized = true
          return o.res
        }
        // 5xx / 网络错误：替换 lastBad 时 cancel 旧坏响应 body 并摘旧 listener（审计修复#2）
        if (lastBad) { cancelBody(lastBad.o); unlisten(lastBad.a) }
        lastBad = { o, a }
        decSlot(a) // 保留 listener：此响应可能作为最终错误返回给上游消费
        const why = o.res ? 'status-' + o.res.status : 'net:' + shortErr(o.err)
        stats.redraws++
        record('redraw', why + ' at ' + (Date.now() - t0) + 'ms')
        fireHedge(why, 'redraw')
      }

      clearTimer()
      stats.failures++
      record('all-failed', attempts.map((x) => x.reason + ':' + (x.outcome?.res ? x.outcome.res.status : 'err')).join(','))
      finalized = true
      if (lastBad?.o.res) return lastBad.o.res // 让上游看到真实 HTTP 错误（如 504）→ dsh-llm-retry 正常接管
      if (lastBad) unlisten(lastBad.a)
      throw lastBad?.o.err ?? new Error('ttft-hedge: no outcome')
    } catch (e) {
      clearTimer()
      for (const x of attempts) discard(x) // 审计修复#3：fail-open 前全量清理（幂等）
      const err = e as { name?: string; message?: string }
      if (finalized || /abort/i.test(String(err?.message)) || String(err?.name) === 'AbortError') throw e
      // fail-open：对冲逻辑自身异常 → 原始 fetch 兜底
      stats.internalErrors++
      record('internal-error', shortErr(e))
      log('internal error, falling back: ' + shortErr(e))
      return origFetch(input as FetchInput, init)
    }
  }

  return { fetch: hedgedFetch, stats, config: cfg, inFlight: () => inFlight }
}
