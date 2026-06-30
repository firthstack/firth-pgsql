# firth-pgsql 设计文档：基于 Neon 开源栈的 Serverless Postgres

> 2026-06-12 · 状态：设计已确认，待实施计划
> "fly" 指 on-the-fly（即时拉起），与 Fly.io 无关。

## 1. 目标与非目标

### 1.1 背景与目标

firth-pgsql 是 FirthStack BaaS 的多租户数据库底座：为每个用户项目提供一个独立的 serverless Postgres，要求三个核心能力：

1. **Scale-to-zero**：空闲项目的计算节点缩到零，新连接到达时秒级唤醒（目标冷启动 1–3 秒），大幅降低海量闲置租户的成本。
2. **数据库分支（Branching）**：秒级创建 copy-on-write 分支，用于开发/预览环境。
3. **存算分离**：数据持久性建立在 S3 + WAL 多副本之上，计算节点完全无状态，存储按实际用量增长。

技术路线：复用 Neon 开源组件（Apache-2.0，Databricks 收购后仍活跃维护），自研控制面补齐 Neon 未开源的部分。**Neon 组件一律使用官方镜像、不改源码**，版本钉死 release tag。

### 1.2 非目标（第一版不做）

- 在线热扩缩 CPU/内存（Neon 云的 NeonVM 方案绑定 K8s+QEMU/KVM，不采用）。第一版用"重启式换档"，M4 后用 K8s in-place pod resize（1.33+）逼近。
- 多 region、跨 region 容灾。
- 多 pageserver 分片与 storage_controller（租户量上来后再引入）。
- 连接池（pgbouncer/内置 pooler）。
- 计费计量（只暴露用量数据接口，计费逻辑归 FirthStack 平台）。

## 2. 架构总览

```
                         ┌──────────────────────────────────┐
  FirthStack 平台 ──────────►  firth-pgsql 控制面 (Go, 自研)       │
  (建项目/建分支/查状态)    │  · 租户/分支/endpoint 管理 API     │
                         │  · proxy 契约: wake_compute 等    │
                         │  · compute 生命周期 + 空闲挂起      │
                         │  · 状态库: 自用 Postgres           │
                         └───────┬──────────────┬───────────┘
                                 │ 起停 compute   │ timeline API
  Client ──► Neon Proxy ─────────┤              │
  (5432/SNI)   │ wake_compute    ▼              ▼
               │          ┌─────────────┐ ┌────────────┐
               └─ 转发 ───►│ Compute 池   │ │ Pageserver │──► S3
                          │ 每租户一个 PG │ │ (多租户)     │  (MinIO 本地
                          │ 空闲即销毁    │ └─────▲──────┘   /AWS S3)
                          └──────┬──────┘       │
                            WAL  │        ┌─────┴──────────┐
                                 └───────►│ Safekeeper ×3  │──► S3
                                          │ (Paxos quorum) │
                                          └────────────────┘
                                  (storage_broker 协调两者间元数据)
```

### 2.1 概念映射

| FirthStack 概念 | Neon 概念 | 说明 |
|---|---|---|
| 项目 | tenant | pageserver 原生多租户，一个 pageserver 服务多个 tenant |
| 项目主库 | 主 timeline | tenant 创建时的根 timeline |
| 分支 | 子 timeline | COW，`POST /v1/tenant/{id}/timeline/` 带 `ancestor_timeline_id` |
| 可连接实例 | endpoint | 一个 endpoint 绑定一条 timeline，对应一个可起停的 compute pod |

### 2.2 数据流

- **写路径**：compute → 3×safekeeper（Paxos quorum 确认即提交）→ pageserver 经 storage_broker 发现并消化 WAL → 生成 layer 文件 → 卸载 S3。
- **读路径**：compute shared_buffers 未命中 → `GetPage@LSN` 请求 pageserver。
- **持久性**：完全不依赖 compute 本地盘。compute 销毁不丢数据——这是 scale-to-zero 安全性的基础。pageserver 本地盘仅是缓存，丢失可从 S3 重建。

### 2.3 连接与唤醒流程

1. 客户端以 `ep-xxx.db.<domain>` 连接 proxy（5432），proxy 从 TLS SNI 取 endpoint 名（无 SNI 时支持 `options=endpoint%3Dep-xxx` 兜底）。
2. proxy 调控制面 `get_endpoint_access_control` 获取 role 的 SCRAM secret 并完成认证。
3. proxy 调控制面 `wake_compute`：compute 在跑则直接返回地址；不在则控制面拉起 compute pod（注入 ComputeSpec），等待 compute_ctl 就绪后返回地址。
4. proxy 转发字节流。空闲挂起后下次连接重复此流程。

## 3. 组件设计

### 3.1 Neon 官方组件（不改源码）

| 组件 | 镜像 | 形态（本地 OrbStack k8s） | 职责 |
|---|---|---|---|
| pageserver | `neondatabase/neon` | StatefulSet ×1 + PVC | 页面服务、layer 管理、S3 卸载；HTTP API 9898 |
| safekeeper | `neondatabase/neon` | StatefulSet ×3 + PVC | WAL quorum 持久化 |
| storage_broker | `neondatabase/neon` | Deployment ×1 | pageserver↔safekeeper 元数据协调 |
| proxy | `neondatabase/neon` | Deployment ×1，Service 暴露 5432 | TLS/SNI 路由、认证、调用控制面契约 |
| compute | `neondatabase/compute-node-v17` | 控制面动态创建的 pod | Postgres + neon 扩展 + compute_ctl |

镜像均为 amd64+arm64 多架构（已验证），Apple Silicon 原生运行。

### 3.2 firth-pgsql 控制面（Go，自研——本项目核心交付物）

四块职责，单二进制部署：

**① FirthStack 北向 API（REST）**
- `POST /projects`：创建 tenant + 主 timeline + 默认 endpoint + 默认 role，返回连接串。
- `POST /projects/{id}/branches`：调 pageserver timeline API 建 COW 分支 + 新 endpoint。
- `DELETE /projects/{id}`、`DELETE …/branches/{bid}`：销毁 compute、删 timeline/tenant。
- `GET` 系列：状态、连接串、用量（存储字节数来自 pageserver API）。

**② Proxy 南向契约**（Neon proxy `--auth-backend=console` 模式期望的三个 HTTP API）
- `GET /get_endpoint_access_control`：返回 role 的 SCRAM secret、allowed_ips 等。
- `GET /wake_compute`：核心唤醒入口，返回 compute 的 `address`（host:port）。
- `GET /endpoints/{endpoint}/jwks`：第一阶段返回空列表；后期对接 FirthStack auth 实现 JWT 直连。

实现前以钉死版本的 proxy 源码（`proxy/src/control_plane/client/cplane_proxy_v1.rs`）为准核对字段，并为契约编写针对真实 proxy 的集成测试。

**③ Compute 生命周期**
- `ComputeRuntime` 接口：`Start(spec) / Stop(id) / Status(id)`。**只实现 `K8sRuntime` 一个**（本地 OrbStack k8s 与 M4 的 EKS 共用，经 client-go 操作 pod）。
- 启动时生成 compute_ctl 的 ComputeSpec：tenant/timeline ID、safekeeper 连接列表、pageserver 地址、role/数据库定义。
- 挂起时调 compute_ctl `/terminate`（优雅停机，flush 最终 LSN 到 safekeeper）后删 pod。

**④ 空闲挂起调度器**
- 后台循环轮询活跃 compute 的连接数与最后活动时间（compute_ctl 状态 API / PG 统计）。
- 空闲超阈值（默认 5 分钟，按 endpoint 可配）→ 触发挂起 → endpoint 标记 `suspended`。

**状态库**：自用 Postgres（本地为普通 pod，AWS 为小 RDS）。存：项目↔tenant/timeline/endpoint 映射、role SCRAM secrets、compute 状态机、挂起策略。

**Endpoint 状态机**：`suspended → starting → running → suspending → suspended`（另有 `failed`）。关键约束：
- **并发唤醒幂等**：多个连接同时触发 `wake_compute` 时，用状态行锁保证只起一个 pod，后到者等待同一结果。
- **挂起/唤醒竞态**：`wake_compute` 遇到 `suspending` 状态时，等待终止完成后重新拉起，不返回将死的地址。

### 3.3 仓库结构

```
firth-pgsql/
├── cmd/controlplane/        # 入口
├── internal/
│   ├── api/                 # FirthStack 北向 REST
│   ├── proxycontract/       # proxy 三个 API 的实现
│   ├── compute/             # ComputeRuntime 接口 + K8sRuntime
│   ├── neonclient/          # pageserver / compute_ctl HTTP 客户端
│   ├── suspend/             # 空闲挂起调度器
│   └── state/               # 状态库访问层 + 状态机
├── deploy/
│   ├── k8s/                 # 本地全栈清单（OrbStack k8s；MinIO、Neon 栈、控制面、状态库）
│   └── aws/                 # M4: Terraform / EKS
└── tests/integration/       # 端到端测试
```

## 4. 本地开发环境（M1–M3 全程本地，无 AWS 依赖）

- **k8s**：OrbStack 自带 k8s（单节点；LoadBalancer 直通 macOS 宿主机，自带 hostpath 存储类）。safekeeper 三副本在单节点上仅为拓扑一致性，反亲和在 M4 EKS 上才真实生效。
- **S3**：集群内 MinIO（Neon 官方 docker-compose 同款方案）。
- **TLS/SNI**：mkcert 自签泛域名证书（如 `*.db.local.test`）喂给 proxy `--tls-cert/--tls-key`。域名 TLD 用 `.test`，避免 `.local`（macOS mDNS 冲突）。
- **泛域名解析**：dnsmasq `address=/db.local.test/127.0.0.1` + `/etc/resolver/test`。
- **M3 完成时的本地闭环验收**：建项目 → psql 经 proxy 连接（触发唤醒）→ 读写 → 空闲自动挂起（pod 消失）→ 再连接自动唤醒、数据完好 → 秒级建分支 → 连分支验证 COW 数据。

## 5. 错误处理与已知风险

| 风险 | 影响 | 对策 |
|---|---|---|
| Neon 无生产自托管文档，官方 compose 仅供测试 | 配置组合需自行摸索 | 版本钉死 release tag；集成测试覆盖升级路径；参考社区项目 NeonD、neon-operator（Molnett）的配置与踩坑记录 |
| pageserver 单点 | 宕机期间读不可用（数据不丢，S3 完整） | 原型期接受；M4 起 EBS 快照 + S3 重建演练；生产期引入 storage_controller + 多 pageserver |
| 挂起/唤醒竞态、并发唤醒 | 连接失败或资源泄漏 | 状态机 + 行锁 + 幂等（见 3.2④） |
| wake_compute 超时（镜像未预热、调度慢） | 客户端连接超时 | 节点预拉镜像（DaemonSet imagePuller）；wake 路径打点监控 P99 |
| safekeeper quorum 丢失（≥2 台不可用） | 写入不可用 | 本地接受；M4 三 AZ 分布 + 告警 |
| proxy 契约随上游版本漂移 | 升级后唤醒失败 | 契约集成测试钉住行为；升级走灰度 |
| 控制面状态库与实际 pod 状态漂移 | 幽灵 compute / 误判 suspended | 调度器周期 reconcile：以 k8s 实际状态为准修正状态库 |

## 6. 测试策略

- **单元测试**：状态机转换（含竞态分支）、ComputeSpec 生成、契约 handler。
- **契约测试**：起真实 Neon proxy 指向控制面，验证三个 API 的字段与行为（防上游漂移）。
- **集成测试**（`tests/integration/`，跑在本地 k8s 上）：第 4 节的完整闭环场景，外加：并发唤醒同一 endpoint、挂起瞬间新连接、pageserver 重启后恢复、分支数据隔离性。
- **性能基线**：冷启动 P50/P99（目标 1–3 秒）、wake_compute 各阶段耗时分解。

## 7. 里程碑

| 里程碑 | 交付物 | 验收标准 |
|---|---|---|
| **M1 存储栈跑通** | deploy/k8s 全栈清单；控制面能建 tenant/timeline；手工起 compute | psql 直连 compute 读写成功，数据落 MinIO |
| **M2 Serverless 闭环** | proxy 接入；wake_compute 契约；空闲挂起调度器 | 连接唤醒→空闲挂起→再唤醒全自动，冷启动 ≤3s |
| **M3 分支与北向 API** | branching、北向 REST、集成测试套件 | 第 4 节本地闭环验收全过；FirthStack 可凭 API 完成全生命周期 |
| **M4 上 AWS** | Terraform；EKS（compute）；EC2 存储栈（safekeeper×3 跨三 AZ、pageserver 用 NVMe 实例）；S3 替换 MinIO；监控告警 | 单 region 多 AZ 跑通同一套验收；K8sRuntime 零改动复用 |

M1–M3 结束即可本地完整演示与对接联调；M4 为纯部署与加固，无新功能代码。

## 8. 设计决策记录

| 决策 | 选择 | 理由 |
|---|---|---|
| 技术路线 | 完整 Neon 栈（vs 普通 PG + 容器起停） | branching 与存算分离是结构性需求，轻量方案无法补齐 |
| 部署目标 | AWS（vs Fly.io） | "fly" 指 on-the-fly；AWS 的 S3/EBS/EKS 无 Fly.io 的 volume 锁宿主机与 500GB 上限问题 |
| compute 编排 | Kubernetes（本地 OrbStack → AWS EKS） | 一个 K8sRuntime 两端复用；in-place pod resize 是后续自动扩缩的最现实路径 |
| 控制面语言 | Go | K8s/AWS 生态最成熟，单二进制部署 |
| 实施顺序 | 本地全闭环（M1–M3）→ AWS（M4） | 风险最小，调试成本最低，控制面代码两环境复用 |
| 连接池 | 第一版不做 | YAGNI；Neon proxy 已处理接入层，池化需求出现后再评估 |
