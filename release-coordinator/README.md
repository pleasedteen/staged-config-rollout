# 本地配置发布协调器

一个仅依赖 Python 3 标准库的本地发布系统。它包含：

- 持久化发布协调器：批次推进、暂停/继续、回退、重试、故障恢复。
- 6 个可独立启动、杀掉、重启的模拟服务：实际生效配置保存在各自 JSON 文件中。
- SQLite 持久化发布状态、批次确认、每服务发布前基线、暂停/回退决定和事件。
- 对端归属校验：端点串到另一台服务时拒绝写入、拒绝确认，实时状态不冒用对端配置。
- 旧运行目录原地核验：`relctl verify` 保留有证据的确认、安全补齐缺漏、把无法核实归属的确认/基线标为待核实。
- 丢失指令、丢失回执、重复指令、延迟、服务失联、协调器被 `SIGKILL`、串端点的故障演示。

## 一条命令跑故障演示

```bash
./run_demo.sh
# 或 python3 -m relcoord.demo
```

演示使用临时目录和随机端口，通常 **5–8 秒**完成（远低于 80 秒），并执行 78 项自检。内容包括：

1. 六个服务初始版本不同（`v1.0.0` / `v1.4.2` / `v0.9.7`）。
2. svc-2 注入指令丢失、回执丢失和延迟；协调器用同一 command ID 重试，服务识别重复且不重复写历史。
3. 服务硬重启后仍提供上一次实际生效配置。
4. 已完成发布可回退，每个服务恢复自己的发布前版本和内容。
5. 暂停决定写入 SQLite；`kill -9` 协调器后，新进程接手并保留暂停决定。
6. 失联服务拖住当前批完成和后续批次；恢复后自动补齐。
7. 进行中发布可回退；已开始批次恢复，未开始批次不动；失联者恢复后才显示回退完成。
8. 回退确认后，迟到的旧发布指令因更低 fence 被 `409 stale_fence` 拒绝。
9. 串端点与重启接手（独立的旧 schema 运行目录）：
   - svc-3 的端点被串到 svc-1，且 svc-3 未启动；新代码在服务端以 `409 identity_mismatch`
     拒绝误投的写入，**不会把 svc-3 的配置写到 svc-1**，也不会把 svc-3 误判为完成；
     `relctl status` 显示 `PEER-MISMATCH` 而非冒用 svc-1 的实时配置。
   - 修正端点后执行 `relctl verify --sync-endpoints`：有持久化命令证据的确认保留，
     “已落盘但未记账”的确认安全补齐，无法核实归属的假确认和污染基线被标为
     `pending_verification`（待核实），阻止推进/回退。
   - `SIGKILL` 协调器后重启接手：幂等重试补齐真实 svc-3 并完成发布；
     未核实基线阻止回退写入，核实后回退才完成。

单元测试：

```bash
python3 -m unittest discover -s tests -v
```

## 手动启动

```bash
cd release-coordinator

# 初始化 ./runtime，启动 6 个服务（127.0.0.1:9301-9306）和协调器
./scripts/start-all.sh

# 查询发布进度和每个服务实际生效的完整配置版本/内容
./relctl status
./relctl status --json

# 停止全部本地进程
./scripts/stop-all.sh
```

也可以把 `RELCOORD_HOME` 指向其他状态目录。

### 独立运行/重启单个服务

```bash
# 前台运行，方便观察；可在另一个终端执行
./scripts/start-service.sh svc-1

# 脚本方式硬重启并保留 JSON 状态
./scripts/restart-service.sh svc-1

# 或手动杀进程后重新执行 start-service.sh
kill -9 "$(cat runtime/pids/svc-1.pid)"
./scripts/start-service.sh svc-1
```

服务状态文件：`runtime/services/<service>.json`。即使进程被 `SIGKILL`，重启后仍从该文件提供上一次实际生效配置。

## 提交发布和批次

目标配置文件格式见 `configs/target-v2.json`：

```json
{
  "version": "v2.0.0",
  "content": { "log_level": "info", "max_connections": 512 }
}
```

批次按参数顺序执行；每个 `--batch` 是一批，逗号分隔。默认要求同组每个服务恰好出现一次：

```bash
./relctl submit configs/target-v2.json \
  --batch svc-1,svc-2 \
  --batch svc-3,svc-4 \
  --batch svc-5,svc-6
```

当前批所有服务都确认实际生效后，协调器才会启动下一批。同组存在 `running`、`paused` 或 `rolling_back` 发布时，新发布会被拒绝；SQLite partial unique index 也强制这一点。

## 暂停、继续、回退

```bash
./relctl pause  --reason "wait for change window"
./relctl status
./relctl resume

./relctl rollback --reason "bad canary"
./relctl status
```

建议用失联服务演示确定的暂停边界：

```bash
kill -9 "$(cat runtime/pids/svc-3.pid)"

./relctl submit configs/target-v3.json \
  --batch svc-1,svc-2,svc-3 \
  --batch svc-4,svc-5 \
  --batch svc-6
./relctl pause

# svc-1/svc-2 可先完成；svc-3 失联，因此第 1 批不会完成，第 2 批不会开始
./relctl status

./scripts/restart-service.sh svc-3
# svc-3 补齐后发布进入 paused；仍不会打开第 2 批
./relctl status

./relctl resume
```

回退规则：

- 发布进行中（包括已暂停）可回退。
- 最近一次 `completed` 且未被新发布取代的发布可回退。
- 只恢复已经开始批次中的服务；每台恢复到该发布创建时记录的自己的 baseline。
- 未开始批次不动。
- 可通信服务先恢复；失联服务在恢复通信后补齐。所有已开始服务确认前，状态保持 `rolling_back`。

## 杀掉协调器并接手

```bash
kill -9 "$(cat runtime/pids/coordinator.pid)"

# 新进程读取同一个 SQLite；已接受的 pause/rollback 决定不丢
./scripts/start-coordinator.sh
```

协调器使用 `runtime/coordinator.lock` 上的 `flock` 防止误启动两个 reconciler；旧进程死亡后锁由内核释放。

## 串端点与旧运行目录核验

每个服务在 `/health`、`/state` 和 `/apply` 响应中声明自己的名字；协调器下发
apply/rollback 时携带目标 `service_name`。因此当两个服务被串到同一端点时：

- 错误的对端在写盘前直接返回 `409 identity_mismatch`，**配置不会写到另一台**；
- 协调器不会把该响应当成目标服务的确认，发布/批次不会因此推进或完成；
- 探测和 `relctl status` 发现自报名字不符时标记 `PEER-MISMATCH`，不把该对端的
  `effective` 配置当作目标服务的实时状态。

旧运行目录（旧 schema，曾可能记录串端点造成的假确认/污染基线）修正
`cluster.json` 端点后，在**原地**执行：

```bash
# 把 cluster.json 中修正后的 URL/state_path 同步进 SQLite，并核验
./relctl verify --sync-endpoints

./relctl verify --json      # 机器可读报告
./relctl verify --strict    # 仍有待核实项时以退出码 3 结束
```

核验对象是“当前活动发布；否则最近一次 `superseded=0` 的发布”。证据来自各服务
**自己的** `services/<name>.json`（即使进程未启动也可读）以及名字校验通过的实时端点：

- 命令 ID 出现在本服务 `seen_commands`/`history` 中：确认归属成立，予以保留；
  若 SQLite 漏记确认，则安全补齐（backfill）；
- 无法用证据归属的已有确认 → `apply_attribution`/`rollback_attribution =
  pending_verification`（文本标 `?` / `待核实`），不计入完成数，不能推进发布；
- 无法在本服务历史/生效配置或自带 initial-state 中得到佐证的回退基线 →
  `baseline_attribution = pending_verification`，协调器**不会**把该基线推给服务恢复，
  回退保持 `rolling_back`，直到基线被修正并核实。

协调器启动时会自动把旧 schema 的既有确认/基线迁移为待核实姿态；apply 会用稳定命令 ID
幂等重试以重新取得归属确认，rollback 则只会在基线归属核实后下发。

命令别名：`reverify`、`doctor`、`repair`、`verify-attribution` 等。

## 故障注入

```bash
./relctl faults --service svc-2 --set '{
  "drop_apply": 1,
  "lose_receipt": 1,
  "delay_seconds": 0.35,
  "delay_next": 2
}'

./relctl faults --service svc-2 --reset
```

字段含义：

- `drop_apply: N`：前 N 次 apply 不写入，返回注入错误；协调器重试。
- `lose_receipt: N`：前 N 次 apply 已持久化，但不返回 HTTP 响应；协调器用同一 command ID 重试。
- `delay_next: N` + `delay_seconds: S`：前 N 次请求延迟 S 秒。
- 直接停止服务进程可模拟真正失联。

直接查询服务实际状态：

```bash
curl -s http://127.0.0.1:9301/state | python3 -m json.tool
```

## 查询输出

```bash
./relctl status            # 实时联系服务；失联时回退显示 SQLite 最后观测值
./relctl status --no-live  # 只读 SQLite
./relctl status --json     # 完整机器可读状态
```

文本输出包含：

- release ID、目标版本、整体状态、当前批次；
- pause/rollback 标志和恢复进度；
- 每批、每服务的 `✓ / … / · / R✓` 标记；
- 每台服务实时或最后持久化的实际版本、fence 和完整 content；
- 最近事件。

## 关键实现约定

- **指令幂等**：每服务/操作有稳定 command ID（如 `apply-<release>-<service>`），服务持久化 `seen_commands`，重复回执/重试返回同一结果且不重复追加配置历史。
- **对端归属**：apply/rollback 携带目标 `service_name`，服务在任何持久化之前拒绝名字不符的指令（`409 identity_mismatch`）；所有响应/状态带自报服务名，协调器与状态查询据此识别串端点。归属列（`apply_attribution`/`rollback_attribution`/`baseline_attribution`）为 `pending_verification` 的确认或基线不得用于推进/恢复。
- **fence 防迟到写**：提交发布分配 apply fence；接受回退分配更大的 rollback fence。服务持久化已见最大 fence，回退确认后更低的旧发布指令返回 `409 stale_fence`。
- **baseline**：提交发布时从 SQLite 最后实际状态（初始化时来自 `data/initial-state.json`）保存每服务自己的版本和内容。
- **并发批次调用**：同一批服务并发下发，避免一个失联服务的网络超时阻塞同批其他服务。

## 目录

```text
relcoord/            Python 包：CLI、协调器、模拟服务、存储、状态渲染、演示
configs/             自带目标配置
data/                自带各服务初始状态
faults/              自带故障演示参数
scripts/             启动/重启/停止脚本
tests/               轻量状态机与 fence 单元测试
run_demo.sh          一条命令端到端故障演示
```
