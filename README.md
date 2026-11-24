# 灰度放量核查 API（FastAPI + Prometheus 直方图）

灰度发布后，值班员需要确认最近的请求里是否**至少 99% 在延迟限时内完成**。
本服务周期性抓取两个本地样例目标暴露的 Prometheus 延迟直方图，按桶计数增量
算出两实例合计达标比例的**上下界**，并给出放量建议。

## 判定规则

每实例取**最近两次合格抓取**，且必须同时满足：

1. 两次抓取都在近 **3 秒**新鲜窗口内；
2. 两次抓取来自**同一进程**（`run_id` / 进程启动时间一致，重启会被识别）；
3. 两次抓取间隔**至少 1 秒**；
4. 每份抓取自身是**同一时刻的完整快照**：所有样本属于同一进程标识，桶计数随
   边界单调不减，`+Inf` 桶与 `_count` 相等，桶计数不超过总数；
5. 两份合格快照相减后，累计增量随边界**单调不减**，且不超过总数增量。

第 4、5 条任一不满足时，该实例会被**隔离**：涉及的两份快照全部作废，在拿到
**两次新的合格同进程抓取**之前不再产出样本。`/check` 会一直等到请求自己的
`max-wait` 到期，届时按证据不足返回，并在 `missing_instances` /
`missing_reasons` 中点名实例与原因。

满足后用两次抓取的**累计桶计数之差**得到窗口内各桶的增量，再按给定延迟阈值
`threshold` 计算达标请求数的上下界：

- 下界：确定落在 `<= threshold` 桶内的请求数；
- 上界：可能落在 `<= threshold` 内的请求数；
- 当阈值恰好等于桶边界时答案确定（上下界相等）；当阈值落在某个桶内部时，
  该桶内的请求无法逐个判断，**保留不确定范围**。

两实例桶边界不同也没关系：各自算出上下界后再相加。

| 条件 | 决策 |
| --- | --- |
| 达标比例**下界 ≥ 99%** | `proceed` 可以放量 |
| 达标比例**上界 < 99%** | `halt` 停止放量 |
| 区间跨越 99%，或两实例窗口合计**零流量** | `inconclusive` 证据不足 |
| 等待样本超时 | `inconclusive`，并在 `missing_instances` 中点名缺证据的实例及原因 |

## 接口

打开 `http://localhost:8000/docs` 可直接在 Swagger UI 填参调用。

### `GET /check`

查询参数：

| 参数 | 含义 |
| --- | --- |
| `threshold` | 延迟阈值（秒，> 0），超过视为不达标 |
| `max-wait` | 最长等待样本时间（秒，0 < x ≤ 60），超时即按缺证据返回 |
| `target` | 可选，达标比例目标，默认 `0.99` |

样本齐了立即返回；缺样本时等待到 `max-wait`。每个请求独立计时，
一个请求超时不会影响其他并发核查；所有并发核查共用同一套抓取样本。

### `GET /stats`

返回每个实例的实际抓取次数（成功/失败）、最近抓取时刻、当前进程 id，
以及当前被选为样本的两次抓取时刻、间隔与窗口请求数。

另外每个实例会报告最近一次数据一致性问题（`instance_issues.<实例>`）：
`code` 为机器可读原因，`reason` 为中文说明，`detail` 给出具体边界/数值，
`active` 表示该问题提出的证据隔离是否仍然生效（需要两次新的合格同进程抓取
才能解除）。顶层 `has_issues` 汇总是否存在任何已记录问题。
`current_ready_samples.<实例>` 未就绪时会附带 `not_ready_reason` 和
`reason_text`（中文原因，含“需等待两次新的合格同进程抓取”）。

可识别的原因包括：进程标识混杂、桶计数随边界下降、总数与 `+Inf` 桶不符、
桶计数超过总数、同一边界重复、两份合格快照相减后累计增量随边界下降、
桶增量超过总数增量、累计计数回退（疑似重启）等。

### `GET /health`

存活探针。

## 样例目标的控制接口

两个样例目标（`sample-a`、`sample-b`）桶边界不同：

- A：`0.05, 0.1, 0.25, 0.5, 1, 2.5, 5`
- B：`0.1, 0.3, 0.7, 1.5, 3, 6`

| 接口 | 作用 |
| --- | --- |
| `POST /inject` | body `{"duration_seconds": 0.23, "count": 1}`，注入请求耗时 |
| `POST /restart` | 模拟进程重启：新 `run_id`、新启动时间、计数清零 |
| `GET /state` | 查看内存中的桶计数与进程信息（含 `outstanding_snapshot_copies`） |
| `GET /metrics` | Prometheus 文本格式直方图 |

### `/metrics` 的一致性保证

- 响应正文来自**一次性深拷贝的不可变快照**（进程标识、每个桶、`_count`、
  `_sum`、抓取序号都属于同一时刻）。返回体编码在释放锁之后进行，因此
  **即使两次抓取都停在正文编码阶段，注入和重启也能立即完成**。
- 写入（inject / restart）在锁内原地更新计数，从不等待编码；每次抓取只保留
  自己那一份快照副本，所以**连续写入时留存的旧桶计数份数不会超过在途抓取数**
  （`/state` 的 `outstanding_snapshot_copies` 可直接观察）。

## 用 docker compose 启动

```bash
docker compose up --build
# verifier  http://localhost:8000/docs
# sample-a  http://localhost:9101/metrics
# sample-b  http://localhost:9102/metrics
```

试跑示例：

```bash
# 每实例注入 99 个 0.2s + 1 个 2s 请求
curl -X POST localhost:9101/inject -H 'content-type: application/json' \
  -d '{"duration_seconds":0.2,"count":99}'
curl -X POST localhost:9101/inject -H 'content-type: application/json' \
  -d '{"duration_seconds":2.0,"count":1}'
curl -X POST localhost:9102/inject -H 'content-type: application/json' \
  -d '{"duration_seconds":0.2,"count":99}'
curl -X POST localhost:9102/inject -H 'content-type: application/json' \
  -d '{"duration_seconds":2.0,"count":1}'

# 等 ~2 秒让下一个抓取窗口覆盖注入流量，然后核查
curl 'localhost:8000/check?threshold=0.5&max-wait=8'
# 模拟进程重启
curl -X POST localhost:9101/restart
# 观察一致性状态与原因（重启窗口会给出 process_restarted）
curl localhost:8000/stats
```

> 注意：核查只看“最近两次抓取之间”的增量，因此注入后要等下一次抓取
> （抓取间隔约 1.05s），流量才会出现在样本窗口里；再下一次抓取后窗口即滑过。

## 不用 Docker 的本地启动

```bash
pip install -r requirements.txt
./run_local.sh
```

## 测试

```bash
pip install -r requirements-dev.txt
pytest -q
```

- `tests/test_histogram.py`：解析、桶增量、上下界、双实例不同桶布局的纯函数测试，
  以及单份快照/两份快照的一致性校验（进程标识混杂、桶计数回退、`+Inf` 不符、
  增量随边界下降等）；
- `tests/test_scraper_selection.py`：新鲜窗口、1 秒间隔、跨进程/计数器回滚的样本选择，
  以及矛盾增量触发的隔离与“两次新抓取后恢复”；
- `tests/test_sample_target.py`：`/metrics` 快照的原子性、写入不被阻塞、
  留存副本数不超过在途抓取数；
- `tests/test_integration.py`：真实 uvicorn 进程的端到端测试，覆盖
  proceed / halt / 不确定区间 / 零流量 / 重启恢复 / 缺实例超时 / 并发共享样本 / 统计；
- `tests/test_consistency_integration.py`：脚本化矛盾数据源下 `/stats` 的实例与原因
  上报、`/check` 到期缺证据、隔离后恢复，以及真实重启与注入交错时 `/metrics`
  始终保持单进程单时刻一致。

## 配置（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `SCRAPE_TARGETS` | `sample-a=http://sample-a:9101/metrics,sample-b=...` | 抓取实例，`名称=URL` 逗号分隔 |
| `SAMPLE_BUCKETS` | A/B 各自默认 | 样例目标的桶边界，逗号分隔 |
| `INSTANCE_ID` | `sample-a` | 样例目标实例标识 |
