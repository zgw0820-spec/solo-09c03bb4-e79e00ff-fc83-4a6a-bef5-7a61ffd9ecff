# 监控数据卷交接 API

在 Kubernetes 上重建监控库（例如 Prometheus / Thanos）时，旧的持久卷不能随旧
声明一起删掉。值班员需要把这块卷**原样**交给重建后的新 PVC。本项目提供：

* 一个 FastAPI 服务，通过 Kubernetes API 完成"卷 → 新声明"的预留与进度查询；
* 一个本地 Kubernetes API 模拟端，用 Docker Compose 一起启动，可在 Swagger 里
  完整走一遍交接；
* 单元测试、HTTP 端到端测试和并发压测脚本。

```
.
├── app/                 # 交接服务
│   ├── main.py          # FastAPI 路由、错误映射、Swagger
│   ├── service.py       # 交接规则（唯一写入点是 PV 的 spec.claimRef）
│   ├── kube.py          # 官方 kubernetes 客户端封装 + 错误分类
│   ├── kube_objects.py  # PV/PVC 字段归一化与比较规则
│   ├── quantity.py      # Kubernetes 容量单位解析（Gi/G/M/m…）
│   ├── schemas.py       # 请求/响应模型
│   └── errors.py        # ApiError → JSON
├── mock_kube/           # 本地 Kubernetes API 模拟端
│   ├── app.py           # core/v1 的 PV/PVC 子集 + 运维夹具接口
│   ├── store.py         # 内存对象存储、乐观并发、绑定器
│   └── seed.py          # 演示用预置场景
├── tests/               # pytest（规则、并发、HTTP、增量轮次）
├── scripts/             # demo、增量轮次演示、并发探针
├── deploy/kubeconfig    # Compose 中服务指向 mock-kube 的 kubeconfig
├── Dockerfile
└── docker-compose.yml
```

## 快速开始

```bash
docker compose up --build
```

* 交接 API 的 Swagger：<http://localhost:8000/docs>
* 模拟 Kubernetes API：<http://localhost:8001/docs>（`/_mock/*` 为运维接口）

`mock-kube` 容器以 `MOCK_SEED=1` 启动，预置了演示场景（UID 固定，可直接复制下面的
请求体）。`handover` 容器用 `deploy/kubeconfig` 通过官方 Python 客户端访问
`http://mock-kube:8001`，走的是真实 HTTP 路径，而不是进程内假实现。

### 在 Swagger 里完成一次交接

`POST /handovers`，请求体：

```json
{
  "pv_name": "pvc-5f0c1c9e-aaaa-4bbb-8ccc-0123456789ab",
  "old_pvc_uid": "6b1d0f2e-1111-4c2a-9c33-aa11bb22cc33",
  "new_pvc": {
    "namespace": "monitoring",
    "name": "prometheus-data-rebuilt",
    "uid": "9c2e4a71-2222-4d3b-8e55-bb33cc44dd55"
  }
}
```

1. 提交 → `202`，`status: pending_binding`（预留已写入 `spec.claimRef`，尚未 Bound）。
2. 再次提交同一请求 → 仍为 `202`，内容不变；**不会重复写 PV**（幂等重试即查进度）。
3. 让绑定完成 —— 模拟端提供 `POST /_mock/reconcile`（增量绑定一轮，见下节）；
   单卷也可用 `POST /_mock/bind/pvc-5f0c1c9e-aaaa-4bbb-8ccc-0123456789ab`
   （真实集群里由 PV controller 自动完成）。
4. 再次提交 → `200`，`status: completed`，PV 与 PVC 均已 Bound 且相互指向。
5. 也可以随时只读查询：`GET /handovers/status?pv_name=…&old_pvc_uid=…&namespace=…&name=…&uid=…`

拒绝路径（同样预置好，可直接试）：

| 请求 | 结果 |
| --- | --- |
| 把同一块卷交给 `prometheus-data-rival`（另一个 UID） | `409 pv_reserved_for_other_claim` |
| 交接回收策略为 `Delete` 的卷 | `409 reclaim_policy_not_retain` |
| 卷模式 `Block` 的卷交给 `Filesystem` 的新 PVC | `409 volume_incompatible` |
| 新 PVC 的 `volumeName` 不是这块卷 | `409 volume_incompatible`（`pvc_prebound_to_pv`） |
| 容量/存储类/访问模式不匹配 | `409 volume_incompatible`，`details.checks` 给出逐项结果 |
| 新 PVC 同名重建（UID 变化）后重放旧请求 | `409 new_pvc_uid_mismatch` |

预置数据可用 `POST http://localhost:8001/_mock/reset` 之外的接口自行扩展：
`POST /_mock/pvs`、`POST /_mock/namespaces/{ns}/pvcs`、
`DELETE /_mock/namespaces/{ns}/pvcs/{name}`、`POST /_mock/reconcile`、`GET /_mock/state`。

## 增量绑定（`POST /_mock/reconcile`）

模拟端的绑定器按**增量轮次**运行，不再每轮扫描全部 PV：

* **登记**：创建、更新、删除 PV，以及创建、删除 PVC 时，只把受该变更影响且
  仍然存在的卷登记到下一轮。PVC 变更通过 `spec.claimRef` 的命名空间/名称索引
  和 PVC 自身的 `spec.volumeName` 直接定位关联卷，不需要遍历 PV。
* **取快照**：每轮开始时只取当时已登记、且仍存在的卷；轮次运行期间发生的
  变更（包括新建、同名重建）留到下一轮。
* **尝试前复核**：对每卷尝试绑定前，比较该卷及其声明的变更计数；若快照之后
  又变化过（同名 PVC/PV 重建也算），本轮不尝试，留到下一轮按最新对象处理。
* **条件不满足不空转**：claimRef/UID 不匹配、PVC 不存在、容量/存储类/访问
  模式/卷模式不满足等都会让本轮放弃该卷；只有相关对象再次变动才会重新登记
  并重试。
* **并发**：轮次只对单个卷短暂持有对象锁，绑定在锁内完成；整轮运行期间
  读取 PV/PVC、`/healthz`、`POST /handovers` 都照常服务。

`POST /_mock/reconcile` 返回本轮明细：

```json
{
  "round": 3,
  "registered": 2,
  "attempted": ["pvc-batch-aaaa-…-0001", "pvc-batch-bbbb-…-0002"],
  "bound": [],
  "deferred": [],
  "skipped": [],
  "pending": 0
}
```

`GET /_mock/reconcile/state` 只读查看下一轮将处理的卷。演示步骤：

```bash
docker compose up --build
python3 scripts/reconcile_demo.py     # 自己造 6 个卷：跑一轮、无改动再跑、改 2 个卷、新建 3 个卷、同名重建、并发可用性
python3 scripts/demo.py               # 进程内按 README 的 13 步走一遍
```

## 交接规则

一次交接请求包含：PV 名、旧 PVC 的 UID、新 PVC 的命名空间 / 名称 / UID。

**准入（写之前校验，任一不满足即 409，且不修改集群）**

1. PV 存在，`spec.persistentVolumeReclaimPolicy == Retain`；
2. PV `status.phase == Released`；
3. PV 当前 `spec.claimRef.uid` 等于请求里的旧 PVC UID（即这块卷确实是从旧声明释放的）；
4. 新 PVC 已存在，且集群中该名称的 PVC UID 等于请求里的新 UID；
5. 新 PVC 通过 `spec.volumeName` **预先指定**该卷；
6. 容量：PV `spec.capacity.storage` ≥ 新 PVC `resources.requests.storage`
   （按 Kubernetes 单位解析后做数值比较，不是字符串比较）；
7. 存储类相同；
8. 访问模式：新 PVC 请求的模式是 PV 支持模式的子集（与 PV controller 的
   `IsSubset` 规则一致）；
9. 卷模式相同（`Filesystem` / `Block`）。

**预留**

预留只写一个字段：PV 的 `spec.claimRef` 指向新 PVC 的 `namespace/name/uid`，并附带
两条注解 `handover.example.com/previous-pvc-uid`、`handover.example.com/reserved-at`
便于审计。写入使用 JSON merge patch，并携带 `metadata.resourceVersion` 和
`metadata.uid` 作为前置条件：

* 两个不同接手方并发提交 → API server 只接受一个，另一个拿到 409 后重新读取，
  发现预留已归属他人，返回 `409 pv_reserved_for_other_claim`；
* 同一接手方并发/重复提交 → 冲突后重新读取，发现 `claimRef` 已是自己，按幂等处理，
  只返回进度，不重复写入；
* 服务本身不保存状态，所有进度都从集群实时读取，因此可多副本部署、随时重启。

**状态判定**

* `pending_binding`（HTTP 202）：预留已生效，但 PV 或 PVC 尚未 Bound，或二者没有
  相互指向；`status_text` 会说明卡在哪一侧。
* `completed`（HTTP 200）：PV `status.phase == Bound`、PVC `status.phase == Bound`、
  PV `claimRef` 的 uid 等于 PVC 的 uid、PVC `spec.volumeName` 等于 PV 名。
* 新 PVC 被同名重建（UID 变化）→ 旧请求 409 `new_pvc_uid_mismatch`，不会被误判为
  完成；新 UID 也不能直接抢走旧预留（`409 pv_reserved_for_other_claim`）。

**错误码**

| HTTP | code | 含义 |
| --- | --- | --- |
| 404 | `pv_not_found` | PV 不存在 |
| 409 | `reclaim_policy_not_retain` | 非 Retain |
| 409 | `volume_not_released` | 非 Released / 无 claimRef |
| 409 | `pv_reserved_for_other_claim` | 已预留给别的声明或旧 UID 不符 |
| 409 | `new_pvc_not_found` / `new_pvc_uid_mismatch` | 新 PVC 不存在 / 同名但 UID 不符 |
| 409 | `volume_incompatible` | 容量、存储类、访问模式、卷模式或预绑定不满足 |
| 409 | `no_such_handover` | 只用只读接口查询未发生过的交接 |
| 409 | `concurrent_update` | 持续并发写入导致预留未完成，可重试 |
| 422 | `invalid_request` | 请求体字段非法 |
| 502/503 | `kube_error` / `kube_unavailable` | 集群访问失败 |

## 接入真实集群

服务默认 `KUBE_MODE=remote`，读取 `KUBECONFIG` 或（Pod 内）使用
`KUBERNETES_SERVICE_HOST` 自动切换到 in-cluster 配置。把 `deploy/kubeconfig` 换成
真实集群的配置（或去掉 volume 挂载、改用 ServiceAccount）即可；`KUBE_REQUEST_TIMEOUT`
控制单次调用超时。`KUBE_MODE=mock` 则完全不访问集群，只适合演示。

镜像里只需要 `kubernetes` 客户端的 RBAC：

```yaml
rules:
  - apiGroups: [""]
    resources: ["persistentvolumes"]
    verbs: ["get", "list", "patch"]   # list 仅用于 /healthz 探活；patch 只改 claimRef
  - apiGroups: [""]
    resources: ["persistentvolumeclaims"]
    verbs: ["get"]
```

## 本地运行（不用 Docker）

```bash
python3 -m pip install -r requirements.txt pytest pytest-asyncio httpx

# 终端 1：模拟 Kubernetes API（预置演示数据）
MOCK_SEED=1 uvicorn mock_kube.app:app --port 8001

# 终端 2：交接服务
KUBE_MODE=remote KUBECONFIG=$PWD/deploy/kubeconfig uvicorn app.main:app --port 8000
```

`deploy/kubeconfig` 里的 server 是 Compose 内部主机名 `mock-kube:8001`；本机直接跑时
改成 `http://127.0.0.1:8001`（或使用 `scripts/demo.py`，它会自动生成临时 kubeconfig）。

## 测试与验证

```bash
python3 -m pytest                      # 62 个用例：规则、并发冲突、HTTP 合约、增量绑定、真实客户端到模拟端的端到端
python3 scripts/demo.py                # 按 README 的 13 步走一遍（含增量轮次），逐条打印
python3 scripts/concurrency_probe.py   # 20 轮 10 方并发交接 + 12 路重试风暴
python3 scripts/reconcile_demo.py      # 对运行中的 Compose 栈演示增量轮次与并发可用性
```

`tests/test_api.py` 的 `live_servers` 固件真的用 uvicorn 起模拟端，再由官方
`kubernetes` 客户端通过 HTTP 访问，覆盖 merge patch 的 Content-Type、`Status`
错误体解析、404/409 分类等真实链路细节。

## 已知限制

* 模拟端只实现本服务用到的 `core/v1` 子集（PV/PVC 读、PV merge patch、夹具接口），
  不是通用 Kubernetes 模拟器；它不会运行 scheduler、kubelet、storage provisioner，
  绑定需要 `POST /_mock/bind/...` 或 `POST /_mock/reconcile` 触发。
* 模拟端的增量登记只存在于内存中，进程重启后需要由下一次变更重新登记；这与
  真实集群里 informer 全量 list 后转为增量的行为等价，但这里没有实现启动时的
  历史回放，因此重启前已变更但未绑定的卷不会自动补跑。
* 轮次按单进程串行执行（`_round_lock`），对象锁只覆盖单卷检查与绑定；模拟端
  没有实现真实 API server 的 watch 与 resourceVersion 语义，同名重建靠内部
  变更计数识别。
* 只处理 `PersistentVolume` 已处于 `Released` 的场景。旧 PVC 仍存在（`Bound`）时
  必须先删除旧工作负载和旧 PVC，本服务不会代劳删除。
* 没有对 `ReadWriteOncePod` 等特殊访问模式做额外校验，按子集规则统一处理。
* 未实现多集群、限流与审计落库；注解只用于人工排查。
