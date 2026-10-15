
// test-hedge.mjs — dsh-ttft-hedge 内核单测（本地 mock，无外网依赖）
// 覆盖：9 正路径 + 审计修复回归（T10 commit 后 abort 桥接 / T11 限流不阻断失败重抽 / T12 无隐藏第三发）
import http from 'node:http'
import { createHedgedFetch } from '../lib/hedge.js'

const hits = {}
const server = http.createServer((req, res) => {
  const u = new URL(req.url, 'http://x')
  const c = u.searchParams.get('case') || 'fast'
  hits[c] = (hits[c] || 0) + 1
  const n = hits[c]
  const finish = (code, bodyText) => { res.writeHead(code, { 'content-type': 'text/plain' }); res.end(bodyText) }
  if (c === 'hedge') { if (n === 1) setTimeout(() => finish(200, 'A-slow'), 5000); else finish(200, 'B-fast') }
  else if (c === 'fail504') { if (n === 1) finish(504, 'gateway timeout'); else finish(200, 'B-after-504') }
  else if (c === 'fail504slow') { if (n === 1) setTimeout(() => finish(504, 'gateway timeout'), 250); else finish(200, 'B-after-504') }
  else if (c === 'fail400') finish(400, 'bad request')
  else if (c === 'slow1200') setTimeout(() => finish(200, 'slow-but-ok'), 1200)
  else if (c === 'slow900') setTimeout(() => finish(200, 'mid'), 900)
  else if (c === 'abort') setTimeout(() => finish(200, 'never'), 10000)
  else if (c === 'streamslow') {
    res.writeHead(200, { 'content-type': 'text/plain' })
    res.write('partial-')
    let i = 0
    const iv = setInterval(() => { i++; if (i > 20) { clearInterval(iv); res.end('done') } else res.write('chunk' + i + '-') }, 150)
    res.on('close', () => clearInterval(iv))
  }
  else if (c === 'netfail') { req.socket.destroy() }
  else setTimeout(() => finish(200, 'fast-ok'), 20)
})
await new Promise(r => server.listen(0, '127.0.0.1', r))
const PORT = server.address().port
const url = (c) => 'http://127.0.0.1:' + PORT + '/v1/messages?case=' + c

const baseCfg = { enabled: true, urlIncludes: ['/v1/messages'], methods: ['POST'], deadlineMs: 500, per10kTokensMs: 1200, maxExtraMs: 20000, maxAttempts: 2, hedgesPerMinute: 100, retriableStatuses: [500, 502, 503, 504] }
const mk = (over) => createHedgedFetch(globalThis.fetch, { ...baseCfg, ...over })

let pass = 0, fail = 0
const check = (name, cond, detail) => { if (cond) { pass++; console.log('PASS ' + name + (detail ? ' | ' + detail : '')) } else { fail++; console.log('FAIL ' + name + ' | ' + detail) } }

// T1: 慢首发 → deadline 对冲 → 二发先到
{
  const h = mk({})
  const t0 = Date.now()
  const res = await h.fetch(url('hedge'), { method: 'POST', body: '{"small":1}' })
  const txt = await res.text()
  const ms = Date.now() - t0
  check('T1 hedge win', txt === 'B-fast' && ms >= 480 && ms < 1500 && h.stats.hedged === 1 && h.stats.hedgeWins === 1, 'txt=' + txt + ' ms=' + ms)
}
// T2: 504 → 立即重抽
{
  const h = mk({})
  const res = await h.fetch(url('fail504'), { method: 'POST', body: '{}' })
  const txt = await res.text()
  check('T2 504 redraw', txt === 'B-after-504' && res.status === 200 && h.stats.redraws >= 1, 'txt=' + txt + ' redraws=' + h.stats.redraws)
}
// T3: 400 → 不对冲原样返回
{
  const h = mk({})
  const res = await h.fetch(url('fail400'), { method: 'POST', body: '{}' })
  await res.text()
  check('T3 400 passthrough', res.status === 400 && h.stats.hedged === 0 && hits['fail400'] === 1, 'hits=' + hits['fail400'])
}
// T4: 快响应 → 无对冲
{
  const h = mk({})
  const res = await h.fetch(url('fast'), { method: 'POST', body: '{}' })
  const txt = await res.text()
  check('T4 fast no hedge', txt === 'fast-ok' && h.stats.hedged === 0 && hits['fast'] === 1, 'hedged=' + h.stats.hedged)
}
// T5: caller abort（header 前）→ AbortError 上抛
{
  const h = mk({})
  const ac = new AbortController()
  setTimeout(() => ac.abort(), 200)
  let threw = null
  try { await h.fetch(url('abort'), { method: 'POST', body: '{}', signal: ac.signal }) } catch (e) { threw = e }
  check('T5 abort propagates', threw && /abort/i.test(threw.name + threw.message), 'err=' + (threw ? threw.name : 'none'))
}
// T6: 非匹配 URL → 透传
{
  const h = mk({})
  const res = await h.fetch('http://127.0.0.1:' + PORT + '/other?case=fast', { method: 'POST', body: '{}' })
  await res.text()
  check('T6 url filter', h.stats.intercepted === 0 && h.stats.passthrough === 1, JSON.stringify({ i: h.stats.intercepted, p: h.stats.passthrough }))
}
// T7: 限流 → 慢请求不对冲
{
  const h = mk({ hedgesPerMinute: 0 })
  const t0 = Date.now()
  const res = await h.fetch(url('slow1200'), { method: 'POST', body: '{}' })
  const txt = await res.text()
  const ms = Date.now() - t0
  check('T7 rate limit', txt === 'slow-but-ok' && h.stats.hedged === 0 && ms >= 1150, 'ms=' + ms)
}
// T8: body 大小缩放 deadline
{
  const h = mk({})
  const big = 'x'.repeat(400000)
  const t0 = Date.now()
  const res = await h.fetch(url('slow900'), { method: 'POST', body: JSON.stringify({ pad: big }) })
  const txt = await res.text()
  const ms = Date.now() - t0
  check('T8 deadline scaling', txt === 'mid' && h.stats.hedged === 0 && ms >= 850 && ms < 1500, 'ms=' + ms)
}
// T9: 非 string body → 透传
{
  const h = mk({})
  const res = await h.fetch(url('fast'), { method: 'POST', body: new Uint8Array([1, 2, 3]) })
  await res.arrayBuffer()
  check('T9 non-string body', h.stats.hedged === 0 && h.stats.passthrough >= 1, 'passthrough=' + h.stats.passthrough)
}
// T10（审计修复#1）: commit 后 caller abort → 胜者 body 流被切断（桥接保留）
{
  const h = mk({})
  const ac = new AbortController()
  const res = await h.fetch(url('streamslow'), { method: 'POST', body: '{}', signal: ac.signal })
  check('T10a headers fast', res.status === 200 && h.stats.hedged === 0, 'status=' + res.status)
  const readP = res.text()
  setTimeout(() => ac.abort(), 300)
  let threw = null
  try { await readP } catch (e) { threw = e }
  check('T10b post-commit abort cuts stream', threw && /abort/i.test(threw.name + threw.message), 'err=' + (threw ? threw.name + ':' + String(threw.message).slice(0, 60) : 'NO-THROW(stream read to completion)'))
}
// T11（审计修复#5）: 投机对冲被限流后，5xx 失败重抽仍独立可用
{
  const h = mk({ deadlineMs: 50, hedgesPerMinute: 0 })
  const res = await h.fetch(url('fail504slow'), { method: 'POST', body: '{}' })
  const txt = await res.text()
  check('T11 redraw independent of hedge rate-limit', txt === 'B-after-504' && h.stats.rateLimited === 1 && h.stats.hedged === 0 && h.stats.redraws === 1, 'txt=' + txt + ' rateLimited=' + h.stats.rateLimited + ' hedged=' + h.stats.hedged + ' redraws=' + h.stats.redraws)
}
// T12（finalized 语义）: 双发全网络错误 → 原样抛错，绝无隐藏第三发
{
  const h = mk({ deadlineMs: 100 })
  const before = hits['netfail'] || 0
  let threw = null
  try { await h.fetch(url('netfail'), { method: 'POST', body: '{}' }) } catch (e) { threw = e }
  await new Promise(r => setTimeout(r, 400)) // 给潜在的隐藏第三发留时间
  const used = (hits['netfail'] || 0) - before
  check('T12 no hidden third attempt', threw !== null && used === 2 && h.stats.internalErrors === 0 && h.stats.failures === 1, 'threw=' + (threw ? threw.name || 'err' : 'none') + ' serverHits=' + used + ' internalErrors=' + h.stats.internalErrors + ' failures=' + h.stats.failures)
}
console.log('')
console.log('RESULT: ' + pass + ' pass, ' + fail + ' fail')
server.close()
process.exit(fail ? 1 : 0)
