# 本地配置发布协调器

一个仅依赖 Python 3 标准库的本地发布系统。它包含：

- 持久化发布协调器：批次推进、暂停/继续、回退、重试、故障恢复。
- 6 个可独立启动、杀掉、重启的模拟服务：实际生效配置保存在各自 JSON 文件中。
- SQLite 持久化发布状态、批次确认、每服务发布前基线、暂停/回退决定和事件。
- 丢失指令、丢失回执、重复指令、延迟、服务失联、协调器被 `SIGKILL` 的故障演示。

## 一条命令跑故障演示

```bash
./run_demo.sh
# 或 python3 -m relcoord.demo
```

演示使用临时目录和随机端口，通常 **3–5 秒**完成（远低于 80 秒），并执行约 50 项自检。内容包括：

1. 六个服务初始版本不同（`v1.0.0` / `v1.4.2` / `v0.9.7`）。
2. svc-2 注入指令丢失、回执丢失和延迟；协调器用同一 command ID 重试，服务识别重复且不重复写历史。
3. 服务硬重启后仍提供上一次实际生效配置。
4. 已完成发布可回退，每个服务恢复自己的发布前版本和内容。
5. 暂停决定写入 SQLite；`kill -9` 协调器后，新进程接手并保留暂停决定。
6. 失联服务拖住当前批完成和后续批次；恢复后自动补齐。
7. 进行中发布可回退；已开始批次恢复，未开始批次不动；失联者恢复后才显示回退完成。
8. 回退确认后，迟到的旧发布指令因更低 fence 被 `409 stale_fence` 拒绝。

单元测试：

```bash
python3 -m unittest discover -s tests -v
```

## 一条命令验证多组隔离和重启接手

```bash
./verify-groups.sh
# 等价：python3 -m relcoord.group_verify
```

该命令只使用临时目录和空闲本地端口，会自动完成 19 项检查：

1. 构造旧的单组 `cluster.json` / SQLite（`services.name PRIMARY KEY`）并原地迁移到 `(group_name, name)`；
2. 在同一个 home 中启动 `core`、`canary` 两组，两组都有同名服务；
3. 启动**一个**协调器管理所有组，验证全局后台探测/写入并发峰值恰好为 8；
4. 卡住 `core/svc-1` 时，`canary` 的首批仍在慢注入结束前确认；
5. 验证 `status/pause/resume/rollback --group ...` 只影响目标组；
6. `kill -9` 协调器后启动新进程，从同一个 SQLite 接手持久 pause 决定；
7. 回退 `canary` 后，`core` 的同名服务仍保持自己的版本。

## 多组使用方式

同一个 `RELCOORD_HOME` 可以重复 init 多个组；组内服务名唯一即可，不同组允许同名。新组的状态文件会放在 `services/<group>/` 下，旧单组的扁平 `services/<name>.json` 路径保持不变。

```bash
H=./runtime-groups

./relctl init --home "$H" --group core \
  --services svc-1,svc-2,svc-3 --ports 9401,9402,9403
./relctl init --home "$H" --group canary \
  --services svc-1,svc-2,svc-3 --ports 9411,9412,9413

# 同名服务进程必须用 --group 区分
python3 -m relcoord service --home "$H" --group core   --name svc-1 &
python3 -m relcoord service --home "$H" --group canary --name svc-1 &

python3 -m relcoord coordinator --home "$H"

./relctl submit configs/target-v2.json --home "$H" --group core \
  --batch svc-1 --batch svc-2,svc-3
./relctl submit configs/target-v3.json --home "$H" --group canary \
  --batch svc-1 --batch svc-2,svc-3

./relctl status   --home "$H" --group canary --json
./relctl status   --home "$H" --all-groups
./relctl pause    --home "$H" --group core
./relctl resume   --home "$H" --group core
./relctl rollback --home "$H" --group canary
```

协调器内只有一个 8 worker 的请求调度器；探测、发布写入和回退写入都经过它，因此整个协调器进程同时最多有 8 个服务 HTTP 请求。各组有独立监管线程和公平队列，一组的慢请求不会阻塞另一组完成自己的批次。

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
