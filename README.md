# fly-pgsql

On-the-fly serverless Postgres for InsForge, built on [Neon](https://github.com/neondatabase/neon)'s
open-source storage stack. Multi-tenant, copy-on-write branching, scale-to-zero.

```
                         ┌──────────────────────────────────┐
  InsForge ──────────────►  控制面 (Go, cmd/controlplane)     │
  (建项目/建分支/用量)      │  · 北向 REST API                  │
                         │  · proxy 契约: wake_compute 等    │
                         │  · compute pod 生命周期 + 空闲挂起  │
                         └───────┬──────────────┬───────────┘
                                 │ 起停 pod       │ timeline API
  psql ────► Neon Proxy ─────────┤              │
  (TLS/SNI)    │ wake_compute    ▼              ▼
               │          ┌─────────────┐ ┌────────────┐
               └─ 转发 ───►│ Compute 池   │ │ Pageserver │──► S3/MinIO
                          │ 每endpoint一个│ │ (多租户)     │
                          │ pod,空闲即销毁│ └─────▲──────┘
                          └──────┬──────┘       │
                            WAL  │        ┌─────┴──────────┐
                                 └───────►│ Safekeeper ×3  │──► S3/MinIO
                                          └────────────────┘
```

概念映射：InsForge 项目 = Neon tenant；分支 = COW timeline；可连接实例 = endpoint（一个可起停的 compute pod）。compute 完全无状态——持久性在 safekeeper quorum + pageserver + S3。

**实测性能**（OrbStack k8s, Apple Silicon）：冷启动（连接触发唤醒到可查询）≈1.3s；热连接 11ms；COW 分支创建 22ms；挂起后重唤醒 ≈3s。

## 快速开始（本地 OrbStack k8s）

前置：OrbStack（启用 k8s）、Go 1.24+、Docker。

```bash
orb start k8s && kubectl config use-context orbstack

make deploy-storage     # MinIO + storage_broker + safekeeper×3 + pageserver + 状态库
make certs              # 自签 CA + *.db.127-0-0-1.sslip.io 泛域名证书 → secret
make deploy-cp          # 构建并部署 Go 控制面
kubectl apply -f deploy/k8s/70-proxy.yaml   # Neon proxy

# 等全部 Running 后：
kubectl -n fly-pgsql port-forward svc/controlplane 18080:8080 &
make forward &          # proxy → localhost:5432

# 建项目（返回连接串，密码仅此一次）
curl -s -X POST localhost:18080/v1/projects -d '{"name":"demo"}' | jq

# 用返回的 connection_uri 直接连（域名经 sslip.io 解析到 127.0.0.1；
# 连接会自动唤醒 compute。验证证书加 sslrootcert）：
psql "postgresql://insforge:<password>@ep-xxxx.db.127-0-0-1.sslip.io:5432/appdb?sslmode=verify-full&sslrootcert=deploy/certs/ca.crt"

# 建分支（COW，毫秒级；凭项目密码连分支的 host）
curl -s -X POST localhost:18080/v1/projects/<prj>/branches -d '{"name":"preview"}' | jq
```

断开连接后空闲超过 `suspend_after_seconds`（默认 300s，建项目时可传）compute pod 自动销毁；下一次连接自动唤醒，数据无损。

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/v1/projects` | 建项目（tenant + main 分支 + endpoint），body: `{"name", "suspend_after_seconds"?}` |
| GET | `/v1/projects/{id}` | 项目详情与分支列表 |
| DELETE | `/v1/projects/{id}` | 删项目（停 compute、删 tenant） |
| POST | `/v1/projects/{id}/branches` | 建分支，body: `{"name", "parent_branch_id"?}` |
| DELETE | `/v1/projects/{id}/branches/{bid}` | 删分支（默认分支拒绝） |
| GET | `/v1/projects/{id}/usage` | 各分支逻辑大小 |
| GET/`proxy` | `/proxy/api/*` | Neon proxy 契约（get_endpoint_access_control / wake_compute / jwks） |

## 测试

```bash
# 单元测试（需要一个测试 postgres）
docker run -d --name flypgsql-test -e POSTGRES_PASSWORD=t -p 5433:5432 postgres:17
TEST_DATABASE_URL=postgres://postgres:t@localhost:5433/postgres make test

# 端到端（需要上面整套集群在跑）
make integration
```

## 版本钉定与已知约束

- 存储镜像 `ghcr.io/neondatabase/neon:release-9129`，compute `ghcr.io/neondatabase/compute-node-v17:release-compute-9073`（均 amd64+arm64）。升级须重跑集成测试（proxy 契约/compute_ctl 行为可能漂移）。
- compute pod 设 `INSTANCE_ID` 走 compute_ctl 的可信网络模式（外部 HTTP API 免 JWT）；安全边界是 pod 网络，公网只暴露 proxy。
- 本地为单 pageserver；宕机期间读不可用但数据不丢（M4 生产化再做冗余）。
- `fsync=off` 等开发参数沿用上游 compose，生产部署（M4/AWS）需要调整。

## 文档

- 设计 spec：`docs/superpowers/specs/2026-06-12-fly-pgsql-design.md`
- 实施计划：`docs/superpowers/plans/2026-06-12-fly-pgsql-m1-m3.md`
