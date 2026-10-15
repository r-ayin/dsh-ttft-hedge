# dsh-ttft-hedge

TTFT 优化：对发往 LLM 中继 /v1/messages 的流式请求做对冲（hedged request）——deadline 内未收到响应头则并行发第二个请求，先到先用；5xx/504 立即重抽；4xx/429 原样返回。附统计工具与 stats 文件。

由 dsh-super-injector dev_scaffold_plugin 生成。

## 构建与注入

```bash
bash scripts/build.sh
# 注入器环境内：dev_inject_plugin <本目录>
```
