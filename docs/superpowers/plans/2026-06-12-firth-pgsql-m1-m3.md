# firth-pgsql M1+M2+M3 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在本地 OrbStack k8s 上完整跑通基于 Neon 开源栈的 serverless Postgres：多租户、branching、scale-to-zero（spec: `docs/superpowers/specs/2026-06-12-firth-pgsql-design.md`）。

**Architecture:** Neon 官方镜像（钉死 `release-9129`）跑存储栈（pageserver + 3×safekeeper + storage_broker + proxy）+ MinIO 替代 S3；自研 Go 控制面实现 proxy 的 wake_compute 契约、compute pod 生命周期（client-go）、空闲挂起调度。

**Tech Stack:** Go 1.24+（stdlib `net/http` + pgx/v5 + client-go）、OrbStack k8s、`ghcr.io/neondatabase/neon:release-9129`、`ghcr.io/neondatabase/compute-node-v17:release-9129`、openssl 自签证书、`*.db.127-0-0-1.sslip.io` 泛域名。

---

## 全局约定（所有任务遵守）

| 项 | 值 |
|---|---|
| k8s namespace | `firth-pgsql` |
| 存储镜像 | `ghcr.io/neondatabase/neon:release-9129`（pageserver/safekeeper/broker/proxy 同一镜像） |
| compute 镜像 | `ghcr.io/neondatabase/compute-node-v17:release-compute-9073`（compute 走独立 release 轨道，无 release-9129 tag；执行期已验证含 arm64） |
| Go module | `github.com/insforge/firth-pgsql` |
| 端口 | pageserver 6400(pg)/9898(http)；safekeeper 5454(pg)/7676(http)；broker 50051；compute 55433(pg)/3080(http)；proxy 4432(pg)/7001(http)；控制面 8080；MinIO 9000；statedb 5432 |
| 域名 | endpoint 域名 `<ep-id>.db.127-0-0-1.sslip.io`（公共 DNS 解析到 127.0.0.1，零配置） |
| ID 格式 | tenant/timeline = 32 hex；project = `prj` + 12 hex；branch = `br-` + 12 hex；endpoint = `ep-` + 12 hex（proxy 只允许字母数字和 `-`） |
| MinIO | bucket `neon`，region `eu-north-1`，AK/SK `minio`/`password`（与上游 compose 一致） |
| cloud_admin | 每个 compute 的超级用户，`encrypted_password` 用 md5 hex `b093c0d3b281ba6da1eacc608620abd8`（即 md5("cloud_admin"+"cloud_admin")，上游同款） |
| endpoint 状态机 | `suspended → starting → running → suspending → suspended`，另有 `failed`；行级 `FOR UPDATE` 保证并发唤醒幂等 |

**执行期验证清单**（研究结论存在版本漂移风险的点，遇到不符立即停下修正计划而不是硬绕）：
1. `docker manifest inspect ghcr.io/neondatabase/neon:release-9129` 和 `compute-node-v17:release-9129` 必须含 arm64；若无 → 改用 Docker Hub `neondatabase/*` 同期 tag 或最近的含 arm64 的 release tag。
2. release-9129 的 proxy 实际请求的契约路径（可能是 `get_endpoint_access_control`/`wake_compute` 或旧名 `proxy_get_role_secret`/`proxy_wake_compute`）——控制面四个别名都注册，M2 契约测试用真 proxy 验证命中哪个。
3. release-9129 的 proxy `--auth-backend` 取值（`cplane-v1` 是长期稳定别名，优先用它；若不识别试 `console`）。
4. compute_ctl `--config` 包裹格式 `{"spec":…,"compute_ctl_config":{"jwks":{"keys":[]}}}` 中空 jwks 是否被接受（用了 `--dev` 应跳过鉴权）；若启动报错 → 用 openssl 生成 Ed25519 JWKS 填入（命令见 Task 11）。
5. compute_ctl `/status` 的 `last_active` 语义（是否随空闲连接更新）——M2 的 e2e 用实测确定挂起判据。

---

## Phase 0：环境与脚手架

### Task 1: 工具链安装与集群启用

**Files:** 无（纯环境操作）

- [ ] **Step 1: 安装 Go**

```bash
brew install go
go version   # 期望 go1.24+ darwin/arm64
```

- [ ] **Step 2: 启用 OrbStack k8s**

```bash
orb start k8s
kubectl config use-context orbstack
kubectl get nodes   # 期望 1 个 Ready 节点（orbstack）
```

- [ ] **Step 3: 验证镜像架构并拉取（执行期验证清单第 1 条）**

```bash
docker manifest inspect ghcr.io/neondatabase/neon:release-9129 | grep -A2 arm64
docker manifest inspect ghcr.io/neondatabase/compute-node-v17:release-9129 | grep -A2 arm64
docker pull ghcr.io/neondatabase/neon:release-9129
docker pull ghcr.io/neondatabase/compute-node-v17:release-9129
```
期望：两镜像均含 `linux/arm64`。若 release-9129 缺 arm64：用 `https://api.github.com/repos/neondatabase/neon/tags` 找最近的 `release-*` 逐个试，更新本计划全局约定中的 tag 并全文替换。

### Task 2: 仓库脚手架

**Files:**
- Create: `go.mod`, `Makefile`, `.gitignore`, `README.md`（占位一句话）

- [ ] **Step 1: 初始化 module 与依赖**

```bash
cd /Users/junwen/Work/InsFg/firth-pgsql
go mod init github.com/insforge/firth-pgsql
go get github.com/jackc/pgx/v5@latest golang.org/x/crypto@latest \
      k8s.io/client-go@latest k8s.io/api@latest k8s.io/apimachinery@latest
```

- [ ] **Step 2: 写 Makefile 与 .gitignore**

`.gitignore`:
```
bin/
deploy/certs/
*.key
*.pem
.env
```

`Makefile`:
```makefile
NS := firth-pgsql

.PHONY: test build image deploy-storage deploy-cp certs forward integration

test:
	go test ./internal/...

build:
	CGO_ENABLED=0 go build -o bin/controlplane ./cmd/controlplane

image:
	docker build -t firth-pgsql/controlplane:dev .

deploy-storage:
	kubectl apply -f deploy/k8s/00-namespace.yaml -f deploy/k8s/10-minio.yaml \
	  -f deploy/k8s/20-storage-broker.yaml -f deploy/k8s/30-safekeeper.yaml \
	  -f deploy/k8s/40-pageserver.yaml -f deploy/k8s/50-statedb.yaml

deploy-cp: image
	kubectl apply -f deploy/k8s/60-controlplane.yaml
	kubectl -n $(NS) rollout restart deploy/controlplane

certs:
	bash scripts/gen-certs.sh

forward:
	kubectl -n $(NS) port-forward svc/proxy 5432:4432

integration:
	go test -tags=integration -count=1 -timeout 30m ./tests/integration/...
```

- [ ] **Step 3: 提交**

```bash
git add -A && git commit -m "chore: scaffold go module and makefile"
```

---

## Phase 1（M1）：存储栈 + 租户/分支管理

### Task 3: 存储栈 k8s 清单与部署

**Files:**
- Create: `deploy/k8s/00-namespace.yaml`, `deploy/k8s/10-minio.yaml`, `deploy/k8s/20-storage-broker.yaml`, `deploy/k8s/30-safekeeper.yaml`, `deploy/k8s/40-pageserver.yaml`, `deploy/k8s/50-statedb.yaml`

- [ ] **Step 1: namespace 与 MinIO**

`00-namespace.yaml`:
```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: firth-pgsql
```

`10-minio.yaml`（Deployment + PVC + Service + bucket 初始化 Job）:
```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: minio-data, namespace: firth-pgsql }
spec: { accessModes: [ReadWriteOnce], resources: { requests: { storage: 20Gi } } }
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: minio, namespace: firth-pgsql }
spec:
  replicas: 1
  selector: { matchLabels: { app: minio } }
  template:
    metadata: { labels: { app: minio } }
    spec:
      containers:
      - name: minio
        image: quay.io/minio/minio:latest
        args: ["server", "/data", "--address", ":9000"]
        env:
        - { name: MINIO_ROOT_USER, value: minio }
        - { name: MINIO_ROOT_PASSWORD, value: password }
        ports: [{ containerPort: 9000 }]
        volumeMounts: [{ name: data, mountPath: /data }]
      volumes: [{ name: data, persistentVolumeClaim: { claimName: minio-data } }]
---
apiVersion: v1
kind: Service
metadata: { name: minio, namespace: firth-pgsql }
spec: { selector: { app: minio }, ports: [{ port: 9000 }] }
---
apiVersion: batch/v1
kind: Job
metadata: { name: minio-create-bucket, namespace: firth-pgsql }
spec:
  template:
    spec:
      restartPolicy: OnFailure
      containers:
      - name: mc
        image: minio/mc:latest
        command: ["/bin/sh", "-c"]
        args:
        - until mc alias set m http://minio:9000 minio password; do sleep 1; done;
          mc mb --ignore-existing m/neon --region eu-north-1
```

- [ ] **Step 2: storage_broker 与 safekeeper**

`20-storage-broker.yaml`:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata: { name: storage-broker, namespace: firth-pgsql }
spec:
  replicas: 1
  selector: { matchLabels: { app: storage-broker } }
  template:
    metadata: { labels: { app: storage-broker } }
    spec:
      containers:
      - name: broker
        image: ghcr.io/neondatabase/neon:release-9129
        command: ["storage_broker", "--listen-addr=0.0.0.0:50051"]
        ports: [{ containerPort: 50051 }]
---
apiVersion: v1
kind: Service
metadata: { name: storage-broker, namespace: firth-pgsql }
spec: { selector: { app: storage-broker }, ports: [{ port: 50051 }] }
```

`30-safekeeper.yaml`（StatefulSet ×3 + headless Service；id = ordinal+1，advertise URL 用 per-pod DNS）:
```yaml
apiVersion: v1
kind: Service
metadata: { name: safekeeper, namespace: firth-pgsql }
spec:
  clusterIP: None
  selector: { app: safekeeper }
  ports: [{ name: pg, port: 5454 }, { name: http, port: 7676 }]
---
apiVersion: apps/v1
kind: StatefulSet
metadata: { name: safekeeper, namespace: firth-pgsql }
spec:
  serviceName: safekeeper
  replicas: 3
  selector: { matchLabels: { app: safekeeper } }
  template:
    metadata: { labels: { app: safekeeper } }
    spec:
      containers:
      - name: safekeeper
        image: ghcr.io/neondatabase/neon:release-9129
        env:
        - { name: AWS_ACCESS_KEY_ID, value: minio }
        - { name: AWS_SECRET_ACCESS_KEY, value: password }
        command: ["/bin/sh", "-c"]
        args:
        - >
          ORD=${HOSTNAME##*-};
          exec safekeeper
          --id=$((ORD+1))
          --listen-pg=${HOSTNAME}.safekeeper.firth-pgsql.svc.cluster.local:5454
          --listen-http=0.0.0.0:7676
          --broker-endpoint=http://storage-broker:50051
          -D /data
          --remote-storage="{endpoint='http://minio:9000',bucket_name='neon',bucket_region='eu-north-1',prefix_in_bucket='/safekeeper/'}"
        ports: [{ containerPort: 5454 }, { containerPort: 7676 }]
        volumeMounts: [{ name: data, mountPath: /data }]
  volumeClaimTemplates:
  - metadata: { name: data }
    spec: { accessModes: [ReadWriteOnce], resources: { requests: { storage: 5Gi } } }
```

- [ ] **Step 3: pageserver（ConfigMap 配置 + initContainer 复制进 PVC，因 -D 目录既放配置也放数据）**

`40-pageserver.yaml`:
```yaml
apiVersion: v1
kind: ConfigMap
metadata: { name: pageserver-config, namespace: firth-pgsql }
data:
  pageserver.toml: |
    broker_endpoint='http://storage-broker:50051'
    pg_distrib_dir='/usr/local/'
    listen_pg_addr='0.0.0.0:6400'
    listen_http_addr='0.0.0.0:9898'
    remote_storage={ endpoint='http://minio:9000', bucket_name='neon', bucket_region='eu-north-1', prefix_in_bucket='/pageserver' }
    control_plane_api='http://0.0.0.0:6666'
    control_plane_emergency_mode=true
  identity.toml: |
    id=1234
---
apiVersion: apps/v1
kind: StatefulSet
metadata: { name: pageserver, namespace: firth-pgsql }
spec:
  serviceName: pageserver
  replicas: 1
  selector: { matchLabels: { app: pageserver } }
  template:
    metadata: { labels: { app: pageserver } }
    spec:
      initContainers:
      - name: copy-config
        image: busybox:1.36
        command: ["/bin/sh", "-c", "cp /config-src/* /data/.neon/ || mkdir -p /data/.neon && cp /config-src/* /data/.neon/"]
        volumeMounts:
        - { name: config, mountPath: /config-src }
        - { name: data, mountPath: /data/.neon, subPath: neon }
      containers:
      - name: pageserver
        image: ghcr.io/neondatabase/neon:release-9129
        command: ["pageserver", "-D", "/data/.neon"]
        env:
        - { name: AWS_ACCESS_KEY_ID, value: minio }
        - { name: AWS_SECRET_ACCESS_KEY, value: password }
        ports: [{ containerPort: 6400 }, { containerPort: 9898 }]
        volumeMounts: [{ name: data, mountPath: /data/.neon, subPath: neon }]
      volumes: [{ name: config, configMap: { name: pageserver-config } }]
  volumeClaimTemplates:
  - metadata: { name: data }
    spec: { accessModes: [ReadWriteOnce], resources: { requests: { storage: 20Gi } } }
---
apiVersion: v1
kind: Service
metadata: { name: pageserver, namespace: firth-pgsql }
spec: { selector: { app: pageserver }, ports: [{ name: pg, port: 6400 }, { name: http, port: 9898 }] }
```
注意：镜像以 `USER neon` 运行，若 initContainer 复制的文件权限导致 pageserver 读不到，给 initContainer 加 `chmod -R a+r`；若 PVC 属主问题导致写失败，在 pod spec 加 `securityContext: { fsGroup: 1000 }`（执行时按实际报错调整，这是 k8s 上跑该镜像最常见的坑）。

- [ ] **Step 4: 状态库**

`50-statedb.yaml`：postgres:17 Deployment + 5Gi PVC + Service `statedb`，`POSTGRES_USER=firthpgsql / POSTGRES_PASSWORD=firthpgsql / POSTGRES_DB=firthpgsql`（结构同 MinIO 的 Deployment 模式，端口 5432）。

- [ ] **Step 5: 部署并验证**

```bash
make deploy-storage
kubectl -n firth-pgsql get pods   # 期望 minio/broker/safekeeper-{0,1,2}/pageserver-0/statedb 全部 Running，Job Completed
kubectl -n firth-pgsql port-forward svc/pageserver 9898:9898 &
curl -s http://localhost:9898/v1/status        # 期望 {"id":1234}
curl -s http://localhost:9898/v1/tenant        # 期望 []
kill %1
```

- [ ] **Step 6: 提交**

```bash
git add deploy/ && git commit -m "feat: k8s manifests for neon storage stack + minio + state db"
```

### Task 4: 状态库 schema 与 store 层

**Files:**
- Create: `internal/state/schema.sql`, `internal/state/store.go`, `internal/state/store_test.go`

测试依赖真实 postgres：测试用 `TEST_DATABASE_URL` 环境变量（本地 `docker run -d --name firthpgsql-test -e POSTGRES_PASSWORD=t -p 5433:5432 postgres:17` 或 port-forward statedb），未设置时 `t.Skip`。

- [ ] **Step 1: 写 schema.sql**

```sql
CREATE TABLE IF NOT EXISTS projects (
  id            text PRIMARY KEY,
  name          text NOT NULL,
  tenant_id     text NOT NULL UNIQUE,
  pg_version    int  NOT NULL DEFAULT 17,
  role_name     text NOT NULL,
  role_verifier text NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS branches (
  id               text PRIMARY KEY,
  project_id       text NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  name             text NOT NULL,
  timeline_id      text NOT NULL,
  parent_branch_id text,
  is_default       boolean NOT NULL DEFAULT false,
  created_at       timestamptz NOT NULL DEFAULT now(),
  UNIQUE (project_id, name)
);
CREATE TABLE IF NOT EXISTS endpoints (
  id                    text PRIMARY KEY,
  branch_id             text NOT NULL UNIQUE REFERENCES branches(id) ON DELETE CASCADE,
  state                 text NOT NULL DEFAULT 'suspended',
  compute_addr          text,
  suspend_after_seconds int  NOT NULL DEFAULT 300,
  last_started_at       timestamptz,
  last_active_at        timestamptz,
  updated_at            timestamptz NOT NULL DEFAULT now()
);
```

- [ ] **Step 2: 写失败测试（store_test.go）**

测试用例（每个独立函数，连接 TEST_DATABASE_URL，t.Cleanup 里 `DROP TABLE ... CASCADE` 后重建保证隔离）：
- `TestMigrateIdempotent`：`Migrate(ctx, pool)` 连续跑两次不报错。
- `TestCreateAndGetProject`：`CreateProject` 插入 project+default branch+endpoint 三行（一个事务），`GetProjectByID` / `GetEndpointByID` 取回字段一致，endpoint 初始 state=`suspended`。
- `TestEndpointStateTransition`：`TransitionEndpoint(ctx, epID, from="suspended", to="starting")` 返回 true；同样参数再跑返回 false（state 已变）。
- `TestLookupByEndpointish`：`GetAccessControl(ctx, "ep-xxx")` 返回 role_name/role_verifier/project_id/branch_id。

- [ ] **Step 3: 运行确认失败**

```bash
TEST_DATABASE_URL=postgres://postgres:t@localhost:5433/postgres go test ./internal/state/ -v
```
期望：编译失败（函数未定义）。

- [ ] **Step 4: 实现 store.go**

核心签名（pgxpool；schema.sql 用 `//go:embed` 嵌入，`Migrate` 直接执行）：
```go
type Store struct{ pool *pgxpool.Pool }
func New(pool *pgxpool.Pool) *Store
func Migrate(ctx context.Context, pool *pgxpool.Pool) error

type Project struct{ ID, Name, TenantID, RoleName, RoleVerifier string; PgVersion int }
type Branch struct{ ID, ProjectID, Name, TimelineID string; ParentBranchID *string; IsDefault bool }
type Endpoint struct {
  ID, BranchID, State string
  ComputeAddr         *string
  SuspendAfterSeconds int
  LastStartedAt, LastActiveAt *time.Time
}

func (s *Store) CreateProject(ctx context.Context, p Project, defaultBranch Branch, ep Endpoint) error // 单事务三表插入
func (s *Store) CreateBranch(ctx context.Context, b Branch, ep Endpoint) error
func (s *Store) GetProjectByID(ctx context.Context, id string) (*Project, error)
func (s *Store) GetEndpointByID(ctx context.Context, id string) (*Endpoint, error)
func (s *Store) ListEndpointsByState(ctx context.Context, state string) ([]Endpoint, error)
func (s *Store) GetAccessControl(ctx context.Context, endpointID string) (roleName, roleVerifier, projectID, branchID string, err error)
// CAS 式状态迁移：UPDATE endpoints SET state=$to, updated_at=now() WHERE id=$id AND state=$from
func (s *Store) TransitionEndpoint(ctx context.Context, id, from, to string) (bool, error)
func (s *Store) SetEndpointRunning(ctx context.Context, id, addr string) error  // state=running, compute_addr, last_started_at=now()
func (s *Store) SetEndpointSuspended(ctx context.Context, id string) error      // state=suspended, compute_addr=NULL
func (s *Store) DeleteProject(ctx context.Context, id string) error
func (s *Store) DeleteBranch(ctx context.Context, id string) error
// 供 wake 使用：SELECT ... FOR UPDATE 包装
func (s *Store) WithEndpointLock(ctx context.Context, id string, fn func(ep *Endpoint, tx pgx.Tx) error) error
```

- [ ] **Step 5: 测试通过后提交**

```bash
go test ./internal/state/ -v   # 期望 PASS
git add internal/state && git commit -m "feat: state store with endpoint state machine"
```

### Task 5: SCRAM verifier 生成

**Files:**
- Create: `internal/scram/scram.go`, `internal/scram/scram_test.go`

- [ ] **Step 1: 写失败测试**

```go
func TestVerifierFormat(t *testing.T) {
    v, err := scram.BuildVerifier("secret-pw")
    // 断言：正则 ^SCRAM-SHA-256\$4096:[A-Za-z0-9+/=]+\$[A-Za-z0-9+/=]+:[A-Za-z0-9+/=]+$
    // 断言：salt 解码后 16 字节，storedKey/serverKey 解码后 32 字节
}
// 关键正确性测试（需要 TEST_DATABASE_URL）：
// 在真实 postgres 上 CREATE ROLE scramtest PASSWORD '<我们生成的 verifier>'，
// 然后用 password 认证方式以该角色连接（pg_hba 默认 scram-sha-256）成功。
func TestVerifierAcceptedByPostgres(t *testing.T) { ... }
```
（在 postgres 里 `CREATE ROLE x PASSWORD 'SCRAM-SHA-256$...'` 会原样存入 rolpassword，随后用明文密码登录成功 ⇔ verifier 数学正确。这是最强的正确性证明。）

- [ ] **Step 2: 运行确认失败**（编译错误）

- [ ] **Step 3: 实现**

```go
package scram

import (
    "crypto/hmac"; "crypto/rand"; "crypto/sha256"
    "encoding/base64"; "fmt"
    "golang.org/x/crypto/pbkdf2"
)

const Iterations = 4096

func BuildVerifier(password string) (string, error) {
    salt := make([]byte, 16)
    if _, err := rand.Read(salt); err != nil { return "", err }
    return buildVerifier(password, salt, Iterations), nil
}

func buildVerifier(password string, salt []byte, iter int) string {
    salted := pbkdf2.Key([]byte(password), salt, iter, 32, sha256.New)
    clientKey := hmacSHA256(salted, "Client Key")
    storedKey := sha256.Sum256(clientKey)
    serverKey := hmacSHA256(salted, "Server Key")
    b64 := base64.StdEncoding.EncodeToString
    return fmt.Sprintf("SCRAM-SHA-256$%d:%s$%s:%s", iter, b64(salt), b64(storedKey[:]), b64(serverKey))
}

func hmacSHA256(key []byte, msg string) []byte {
    h := hmac.New(sha256.New, key); h.Write([]byte(msg)); return h.Sum(nil)
}
```
另加 `func RandomPassword() (string, error)`：24 字节 crypto/rand → base64.RawURLEncoding。

- [ ] **Step 4: 测试通过，提交** `git commit -m "feat: scram-sha-256 verifier generation"`

### Task 6: pageserver HTTP 客户端

**Files:**
- Create: `internal/neonclient/pageserver.go`, `internal/neonclient/pageserver_test.go`

- [ ] **Step 1: 写失败测试**（`httptest.Server` 断言 method/path/body，返回录制的响应）

用例：`TestAttachTenant`（PUT `/v1/tenant/{tid}/location_config`，body `{"mode":"AttachedSingle","generation":1,"tenant_conf":{}}`）、`TestCreateRootTimeline`（POST `/v1/tenant/{tid}/timeline/`，body `{"new_timeline_id":"...","pg_version":17}`，断言 201 解析）、`TestCreateBranchTimeline`（body 含 `ancestor_timeline_id`）、`TestGetTimelineDetail`（解析 `current_logical_size`/`last_record_lsn`）、`TestDeleteTimeline`（202 不报错）、`TestDeleteTenant`。

- [ ] **Step 2: 确认失败 → Step 3: 实现**

```go
type PageserverClient struct{ baseURL string; hc *http.Client }
func NewPageserver(baseURL string) *PageserverClient

func (c *PageserverClient) AttachTenant(ctx context.Context, tenantID string) error
func (c *PageserverClient) CreateTimeline(ctx context.Context, tenantID, timelineID string, pgVersion int) error
func (c *PageserverClient) CreateBranch(ctx context.Context, tenantID, newTimelineID, ancestorTimelineID string) error
type TimelineDetail struct {
    TimelineID         string `json:"timeline_id"`
    LastRecordLSN      string `json:"last_record_lsn"`
    CurrentLogicalSize uint64 `json:"current_logical_size"`
}
func (c *PageserverClient) GetTimeline(ctx context.Context, tenantID, timelineID string) (*TimelineDetail, error)
func (c *PageserverClient) DeleteTimeline(ctx context.Context, tenantID, timelineID string) error // 202 后轮询 GET 至 404，最长 60s
func (c *PageserverClient) DeleteTenant(ctx context.Context, tenantID string) error
```
非 2xx 一律返回 `fmt.Errorf("pageserver %s %s: %d: %s", method, path, code, body)`。

- [ ] **Step 4: 测试通过，提交** `git commit -m "feat: pageserver http client"`

### Task 7: ComputeSpec 生成

**Files:**
- Create: `internal/compute/spec.go`, `internal/compute/spec_test.go`

- [ ] **Step 1: 写失败测试**

`TestBuildComputeConfig`：调用 `BuildComputeConfig(SpecParams{...})`，将结果 `json.Marshal` 后断言关键字段：`spec.format_version==1.0`、`spec.suspend_timeout_seconds==-1`、`spec.tenant_id/timeline_id` 正确、`spec.mode=="Primary"`、`spec.safekeeper_connstrings` 为三个 `safekeeper-N.safekeeper:5454`、`spec.pageserver_connstring=="host=pageserver port=6400"`、roles 含 cloud_admin（md5）与项目 role（SCRAM verifier 原样）、databases 含 `{name:"appdb",owner:"<role>"}`、settings 含 `port=55433`、`shared_preload_libraries` 含 `neon`、`password_encryption==scram-sha-256`、`synchronous_standby_names==walproposer`、顶层有 `compute_ctl_config.jwks.keys==[]`。

- [ ] **Step 2: 确认失败 → Step 3: 实现**

```go
type SpecParams struct {
    TenantID, TimelineID   string
    RoleName, RoleVerifier string // verifier 直接作为 encrypted_password（SCRAM 前缀 compute_ctl 原样使用）
    DatabaseName           string // "appdb"
    PageserverConnstring   string // "host=pageserver port=6400"
    Safekeepers            []string
}
func BuildComputeConfig(p SpecParams) ComputeConfig
```
结构体完整定义（serde 字段名与研究报告一致）：
```go
type ComputeConfig struct {
    Spec             ComputeSpec      `json:"spec"`
    ComputeCtlConfig ComputeCtlConfig `json:"compute_ctl_config"`
}
type ComputeCtlConfig struct{ Jwks Jwks `json:"jwks"` }
type Jwks struct{ Keys []any `json:"keys"` }
type ComputeSpec struct {
    FormatVersion         float64   `json:"format_version"`
    SuspendTimeoutSeconds int64     `json:"suspend_timeout_seconds"`
    Cluster               Cluster   `json:"cluster"`
    DeltaOperations       []any     `json:"delta_operations"`
    TenantID              string    `json:"tenant_id"`
    TimelineID            string    `json:"timeline_id"`
    PageserverConnstring  string    `json:"pageserver_connstring"`
    SafekeeperConnstrings []string  `json:"safekeeper_connstrings"`
    Mode                  string    `json:"mode"`
    SkipPgCatalogUpdates  bool      `json:"skip_pg_catalog_updates"`
}
type Cluster struct {
    ClusterID string    `json:"cluster_id"`
    Name      string    `json:"name"`
    State     string    `json:"state"`
    Roles     []Role    `json:"roles"`
    Databases []Database `json:"databases"`
    Settings  []Setting `json:"settings"`
}
type Role struct {
    Name              string  `json:"name"`
    EncryptedPassword *string `json:"encrypted_password"`
    Options           any     `json:"options"`
}
type Database struct {
    Name    string `json:"name"`
    Owner   string `json:"owner"`
    Options any    `json:"options"`
}
type Setting struct{ Name, Value, Vartype string } // json: name/value/vartype
```
settings 固定表（vartype 按上游 compose）：fsync=off(bool)、wal_level=logical(enum)、wal_log_hints=on(bool)、log_connections=on(bool)、port=55433(integer)、shared_buffers=128MB(string)、max_connections=100(integer)、listen_addresses=0.0.0.0(string)、max_wal_senders=10(integer)、max_replication_slots=10(integer)、wal_sender_timeout=5s(string)、wal_keep_size=0(integer)、password_encryption=scram-sha-256(enum)、restart_after_crash=off(bool)、synchronous_standby_names=walproposer(string)、shared_preload_libraries=neon,pg_stat_statements(string)、max_replication_write_lag=500MB(string)、max_replication_flush_lag=10GB(string)。
cloud_admin 的 EncryptedPassword 固定 `"b093c0d3b281ba6da1eacc608620abd8"`。

- [ ] **Step 4: 测试通过，提交** `git commit -m "feat: compute spec builder"`

### Task 8: K8sRuntime（compute pod 生命周期）

**Files:**
- Create: `internal/compute/runtime.go`, `internal/compute/k8s.go`, `internal/compute/k8s_test.go`

- [ ] **Step 1: 写失败测试**（`k8s.io/client-go/kubernetes/fake` 假客户端）

- `TestStartCreatesConfigMapAndPod`：`Start(ctx, "ep-abc", cfg)` 后，fake 集群中存在 `compute-ep-abc-config` ConfigMap（data 含 config.json 且能反序列化回 ComputeConfig）与 `compute-ep-abc` Pod；断言 Pod 的 image、command（compute_ctl 全参数）、volumeMounts、labels（`app=compute, endpoint=ep-abc`）。
- `TestStartIdempotent`：已存在同名 pod 时 Start 不报错（AlreadyExists 容忍）。
- `TestStopDeletesBoth`：Stop 后 pod 与 configmap 不存在；对不存在的 endpoint Stop 不报错（NotFound 容忍）。
- `TestStatusReportsPodIP`：fake pod 设 `status.podIP=10.0.0.5, phase=Running` 后 `Status` 返回 `{Exists: true, PodIP: "10.0.0.5"}`。

- [ ] **Step 2: 确认失败 → Step 3: 实现**

```go
type Runtime interface {
    Start(ctx context.Context, endpointID string, cfg ComputeConfig) error
    Stop(ctx context.Context, endpointID string) error
    Status(ctx context.Context, endpointID string) (PodStatus, error)
}
type PodStatus struct{ Exists bool; PodIP string; Phase string }

type K8sRuntime struct {
    client       kubernetes.Interface
    namespace    string
    computeImage string
}
```
Pod spec 要点（写死在 `buildPod` 函数，完整字段）：
```go
command := []string{
    "/usr/local/bin/compute_ctl",
    "--pgdata", "/var/db/postgres/compute",
    "-C", "postgresql://cloud_admin@localhost:55433/postgres",
    "-b", "/usr/local/bin/postgres",
    "--compute-id", "compute-" + endpointID,
    "--config", "/config/config.json",
    "--dev",
}
```
volumes：configmap `compute-<ep>-config` 挂 `/config`；emptyDir 挂 `/var/db/postgres/compute`。resources：requests `{cpu: 250m, memory: 512Mi}`，limits `{cpu: "1", memory: 1Gi}`。`imagePullPolicy: IfNotPresent`。labels `{app: compute, endpoint: <ep>}`。

- [ ] **Step 4: 测试通过，提交** `git commit -m "feat: k8s compute runtime"`

### Task 9: 北向 API（项目创建）+ main.go 组装

**Files:**
- Create: `internal/api/api.go`, `internal/api/api_test.go`, `internal/ids/ids.go`, `cmd/controlplane/main.go`, `Dockerfile`, `deploy/k8s/60-controlplane.yaml`

- [ ] **Step 1: ids 包**（无测试价值的薄工具，直接写）

```go
package ids
func NewHex32() string      // 16 字节 crypto/rand → hex（tenant/timeline）
func NewProjectID() string  // "prj" + 6 字节 hex
func NewBranchID() string   // "br-" + 6 字节 hex
func NewEndpointID() string // "ep-" + 6 字节 hex
```

- [ ] **Step 2: 写失败测试（api_test.go）**

pageserver 用 httptest 假服务，store 用真测试库，runtime 用 fake：
- `TestCreateProject`：`POST /v1/projects` body `{"name":"demo"}` → 201，响应含 `project_id/branch_id/endpoint_id/role/password/host/database`；store 中三行存在；假 pageserver 收到 location_config PUT + timeline POST。
- `TestCreateProjectRollbackOnPageserverError`：假 pageserver 返回 500 → 接口 502，store 无残留行。
- `TestGetProject`：返回项目与分支列表。

- [ ] **Step 3: 确认失败 → Step 4: 实现 api.go**

```go
type Server struct {
    Store      *state.Store
    Pageserver *neonclient.PageserverClient
    Runtime    compute.Runtime
    Cfg        Config // Domain, PageserverConnstring, Safekeepers, ProxyPort
}
func (s *Server) Routes() *http.ServeMux
```
`POST /v1/projects` 流程：生成 ids 与密码 → `scram.BuildVerifier` → pageserver `AttachTenant` + `CreateTimeline`（失败则直接返回，无需回滚 pageserver——tenant 残留无害，记日志）→ `store.CreateProject` → 201 返回：
```json
{"project_id":"prj…","branch_id":"br-…","endpoint_id":"ep-…",
 "role":"insforge","password":"<明文，仅此一次>",
 "host":"ep-….db.127-0-0-1.sslip.io","port":5432,"database":"appdb",
 "connection_uri":"postgresql://insforge:<pw>@ep-….db.127-0-0-1.sslip.io:5432/appdb?sslmode=require"}
```
调试端点（M1 验收用，M2 复用其内部函数）：`POST /v1/debug/endpoints/{id}/start` → 同步拉起 compute 并等就绪，返回 pod IP；`POST /v1/debug/endpoints/{id}/stop`。start 的就绪等待：每 500ms GET `http://<podIP>:3080/status` 直到 `{"status":"running"}`，超时 120s。

- [ ] **Step 5: main.go + Dockerfile + 部署清单**

main.go：读环境变量 `DATABASE_URL / NAMESPACE / PAGESERVER_URL / PAGESERVER_CONNSTRING / SAFEKEEPERS(逗号分隔) / COMPUTE_IMAGE / DOMAIN / LISTEN(:8080)`；k8s client 先试 `rest.InClusterConfig()` 失败回退 kubeconfig（本地 `go run` 可用）；启动时 `state.Migrate`。

Dockerfile：
```dockerfile
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /controlplane ./cmd/controlplane
FROM alpine:3.20
COPY --from=build /controlplane /controlplane
ENTRYPOINT ["/controlplane"]
```

`60-controlplane.yaml`：ServiceAccount `controlplane` + Role（pods/configmaps 的 get/list/create/delete/watch）+ RoleBinding + Deployment（image `firth-pgsql/controlplane:dev`，`imagePullPolicy: IfNotPresent`，env 上述全套，`DATABASE_URL=postgres://firthpgsql:firthpgsql@statedb:5432/firthpgsql`）+ Service `controlplane:8080`。

- [ ] **Step 6: 测试通过，部署，提交**

```bash
go test ./internal/... && make deploy-cp
kubectl -n firth-pgsql port-forward svc/controlplane 8080:8080 &
curl -s -X POST localhost:8080/v1/projects -d '{"name":"demo"}'   # 期望 201 JSON
git add -A && git commit -m "feat: northbound api + controlplane deployment"
```

### Task 10: M1 端到端验收

**Files:**
- Create: `tests/integration/m1_test.go`（build tag `integration`）

- [ ] **Step 1: 写验收测试**

前置（README 化）：`make deploy-storage deploy-cp` 完成、控制面与 statedb 的 port-forward 在测试 TestMain 里自动建（exec kubectl）或要求手工开。流程：
1. `POST /v1/projects` → 拿到 endpoint_id、password。
2. `POST /v1/debug/endpoints/{ep}/start` → 拿 pod IP。
3. `kubectl -n firth-pgsql port-forward pod/compute-<ep> 55433:55433`（测试内 exec），pgx 连 `postgresql://insforge:<pw>@localhost:55433/appdb` → `CREATE TABLE t(x int); INSERT 1,2,3; SELECT count(*)`==3。
4. 断言 MinIO 有数据：exec `kubectl -n firth-pgsql exec deploy/minio -- ls /data/neon/pageserver/` 非空（或 mc ls）。
5. `POST /v1/debug/endpoints/{ep}/stop` → pod 消失；再 start → 数据仍在（count==3）。**这一步证明存算分离成立，是 M1 的核心验收。**

- [ ] **Step 2: 跑通**

```bash
make integration   # 期望 PASS；首次 compute 镜像拉取慢属正常
```
此处最可能踩 Neon 配置坑（pageserver 权限、safekeeper 连通、spec 字段），逐个解决并把修正回写到清单/代码。

- [ ] **Step 3: 提交** `git commit -m "test: M1 e2e — project create, compute write, data survives compute restart"`

---

## Phase 2（M2）：proxy 接入 + scale-to-zero

### Task 11: TLS 证书与 proxy 部署

**Files:**
- Create: `scripts/gen-certs.sh`, `deploy/k8s/70-proxy.yaml`

- [ ] **Step 1: 证书脚本**（CN 必须是泛域名——proxy 由 CN 推导 SNI common name，mkcert 不满足，故用 openssl）

```bash
#!/usr/bin/env bash
set -euo pipefail
DIR="$(dirname "$0")/../deploy/certs"; mkdir -p "$DIR"; cd "$DIR"
DOMAIN="db.127-0-0-1.sslip.io"
openssl genrsa -out ca.key 2048
openssl req -new -x509 -days 3650 -key ca.key -subj "/CN=firth-pgsql-dev-ca" -out ca.crt
openssl genrsa -out proxy.key 2048
openssl req -new -key proxy.key -subj "/CN=*.${DOMAIN}" -out proxy.csr
printf "subjectAltName=DNS:*.%s,DNS:%s\n" "$DOMAIN" "$DOMAIN" > ext.cnf
openssl x509 -req -days 3650 -in proxy.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -extfile ext.cnf -out proxy.crt
kubectl -n firth-pgsql create secret tls proxy-tls --cert=proxy.crt --key=proxy.key --dry-run=client -o yaml | kubectl apply -f -
echo "CA: $DIR/ca.crt（psql 用 sslrootcert 指向它可 verify-full）"
```
（执行期验证清单第 4 条的兜底——若 compute_ctl 拒绝空 jwks，在此脚本追加：`openssl genpkey -algorithm Ed25519 -out jwt.key && openssl pkey -in jwt.key -pubout -out jwt.pub`，将公钥转 JWKS JSON 注入 spec builder。）

- [ ] **Step 2: proxy 清单**

`70-proxy.yaml`（Deployment + Service）：
```yaml
apiVersion: apps/v1
kind: Deployment
metadata: { name: proxy, namespace: firth-pgsql }
spec:
  replicas: 1
  selector: { matchLabels: { app: proxy } }
  template:
    metadata: { labels: { app: proxy } }
    spec:
      containers:
      - name: proxy
        image: ghcr.io/neondatabase/neon:release-9129
        command:
        - proxy
        - --auth-backend=cplane-v1
        - --auth-endpoint=http://controlplane:8080/proxy/api
        - --proxy=0.0.0.0:4432
        - --http=0.0.0.0:7001
        - --mgmt=0.0.0.0:7000
        - -c=/certs/tls.crt
        - -k=/certs/tls.key
        ports: [{ containerPort: 4432 }, { containerPort: 7001 }]
        volumeMounts: [{ name: tls, mountPath: /certs }]
      volumes: [{ name: tls, secret: { secretName: proxy-tls } }]
---
apiVersion: v1
kind: Service
metadata: { name: proxy, namespace: firth-pgsql }
spec: { selector: { app: proxy }, ports: [{ name: pg, port: 4432 }, { name: http, port: 7001 }] }
```
（执行期验证清单第 3 条：若 `cplane-v1` 不被该版本识别，`kubectl logs` 看 clap 报错中列出的合法值，依次试 `control-plane`、`console`。）

- [ ] **Step 3: 部署验证**

```bash
bash scripts/gen-certs.sh && kubectl apply -f deploy/k8s/70-proxy.yaml
kubectl -n firth-pgsql port-forward svc/proxy 7001:7001 &
curl -s http://localhost:7001/v1/status && kill %1   # 期望 200
git add scripts deploy && git commit -m "feat: tls certs + neon proxy deployment"
```

### Task 12: proxy 契约 handlers

**Files:**
- Create: `internal/proxycontract/handlers.go`, `internal/proxycontract/handlers_test.go`
- Modify: `internal/api/api.go`（挂载 `/proxy/api/` 路由）

- [ ] **Step 1: 写失败测试**（httptest 直测 handler；wake 依赖注入接口 `Waker`，测试用假实现）

- `TestGetEndpointAccessControl`：`GET /proxy/api/get_endpoint_access_control?session_id=u&application_name=psql&endpointish=ep-abc&role=insforge` → 200 `{"role_secret":"SCRAM-SHA-256$…","project_id":"prj…","allowed_ips":null}`。
- `TestAccessControlUnknownEndpoint` → 404，body 为研究报告中的错误结构：`{"error":"endpoint not found","status":{"code":"NOT_FOUND","message":"endpoint not found","details":{"error_info":{"reason":"ENDPOINT_NOT_FOUND"}}}}`。
- `TestAccessControlUnknownRole` → 200 且 `role_secret:""`（proxy 端语义＝无此角色，认证必败，不泄露角色存在性）。
- `TestWakeCompute`：假 Waker 返回 `10.0.0.7:55433` → 200 `{"address":"10.0.0.7:55433","aux":{"endpoint_id":"ep-abc","project_id":"prj…","branch_id":"br-…","compute_id":"compute-ep-abc","cold_start_info":"unknown"}}`。
- `TestWakeComputeFailure`：假 Waker 报错 → 500 + 错误结构（`reason` 留空对象即可，proxy 走 Unknown）。
- `TestLegacyAliases`：`/proxy/api/proxy_get_role_secret` 与 `/proxy/api/proxy_wake_compute` 路由到相同 handler。
- `TestJwks`：`GET /proxy/api/endpoints/ep-abc/jwks` → 200 `{"jwks":[]}`。

- [ ] **Step 2: 确认失败 → Step 3: 实现**

```go
type Waker interface {
    Wake(ctx context.Context, endpointID string) (addr string, err error)
}
type Handlers struct {
    Store *state.Store
    Waker Waker
}
func (h *Handlers) Register(mux *http.ServeMux) // 四条路径 + jwks
```
响应/错误 DTO 按测试中的 JSON 逐字段定义。endpointish 直接当 endpoint id 用（我们生成的 ep-xxx 不含 legacy 后缀）。

- [ ] **Step 4: 测试通过，提交** `git commit -m "feat: proxy control-plane contract endpoints"`

### Task 13: wake 状态机

**Files:**
- Create: `internal/wake/wake.go`, `internal/wake/wake_test.go`
- Modify: `internal/api/api.go`（debug start/stop 改走 wake/suspendOne）、`cmd/controlplane/main.go`（组装）

- [ ] **Step 1: 写失败测试**（真测试库 + fake runtime；fake runtime 的 Status 可编程返回序列）

- `TestWakeFromSuspended`：state=suspended → Wake 返回 addr，最终 state=running、compute_addr 已写入、runtime 收到 Start。
- `TestWakeWhenRunningHealthy`：state=running 且 fake Status 返回 pod 存在 → 直接返回 addr，runtime 无新 Start。
- `TestWakeWhenRunningButPodGone`：state=running 但 pod 不存在 → 修正为 suspended 后重新拉起（reconcile-on-wake）。
- `TestConcurrentWakeSingleStart`：10 个 goroutine 同时 Wake 同一 endpoint → 全部拿到相同 addr，fake runtime 的 Start 仅被调用 1 次。
- `TestWakeStaleStarting`：state=starting 且 updated_at 早于 2 分钟（测试里直接 UPDATE 做旧）→ 接管重启。
- `TestWakeStartFailure`：fake runtime Start 报错 → state=failed，Wake 返回错误；再次 Wake 可从 failed 重试。

- [ ] **Step 2: 确认失败 → Step 3: 实现**

```go
type Waker struct {
    Store        *state.Store
    Runtime      compute.Runtime
    SpecBuilder  func(endpointID string) (compute.ComputeConfig, error) // 闭包查 store 拼 SpecParams
    ReadyTimeout time.Duration // 默认 120s
    StatusURL    func(podIP string) string // http://<ip>:3080/status
}
func (w *Waker) Wake(ctx context.Context, endpointID string) (string, error)
```
算法（循环直至成功/超时）：
1. `WithEndpointLock`（FOR UPDATE 事务）读 state：
   - `running`：事务内直接返回 addr（健康校验放事务外：Status 不存在则 CAS running→suspended 后 continue）。
   - `starting`/`suspending`：若 `updated_at` 距今 >2min 视为 stale，CAS 接管为 starting 并走拉起路径；否则事务外 sleep 500ms 后 continue（等待别人完成）。
   - `suspended`/`failed`：CAS → starting，事务提交后本 goroutine 成为 owner 走拉起路径。
2. owner 拉起路径：SpecBuilder → Runtime.Start → 轮询 PodIP + GET :3080/status 至 `"running"`（500ms 间隔，ReadyTimeout 截止）→ `SetEndpointRunning(id, podIP+":55433")` → 返回 addr。失败：Runtime.Stop 清理、CAS starting→failed、返回 err。

- [ ] **Step 4: 测试通过，提交** `git commit -m "feat: wake state machine with concurrent-safe single start"`

### Task 14: e2e——经 proxy 的冷唤醒

**Files:**
- Create: `tests/integration/m2_wake_test.go`

- [ ] **Step 1: 写测试**

pgx 直连 `127.0.0.1:5432`（port-forward svc/proxy 4432）但 TLS 用 `tls.Config{ServerName: "<ep>.db.127-0-0-1.sslip.io", RootCAs: <ca.crt>}`——不依赖本地 DNS：
1. 建项目（不 debug-start，endpoint 处于 suspended）。
2. pgx 连接 → 应触发 proxy→wake_compute→pod 拉起 → 连接成功，`SELECT 1`。记录耗时（冷启动）。
3. 断开重连 → 热路径，耗时应 <1s。
4. 日志输出冷/热耗时供 M2 验收记录。

- [ ] **Step 2: 跑通提交**

```bash
make integration
git commit -m "test: M2 e2e cold wake through neon proxy"
```
此任务是契约的真实验证点（执行期验证清单第 2/3 条在此闭环）。若 proxy 命中了 legacy 路径或字段不符，按 proxy 实际行为修 handlers 并回写 Task 12 的测试。

### Task 15: 空闲挂起调度器 + reconciler

**Files:**
- Create: `internal/suspend/suspend.go`, `internal/suspend/suspend_test.go`
- Modify: `cmd/controlplane/main.go`（启动调度 goroutine，env `SUSPEND_CHECK_INTERVAL` 默认 30s）

- [ ] **Step 1: 写失败测试**（fake runtime + httptest 假 compute /status/terminate + 真测试库）

- `TestSuspendIdleEndpoint`：running endpoint，假 compute `/status` 返回 `last_active` 为 10 分钟前（suspend_after=300）→ `Sweep(ctx)` 后：terminate 被调（mode=fast）、runtime.Stop 被调、state=suspended、compute_addr=NULL。
- `TestKeepActiveEndpoint`：`last_active` 为 10 秒前 → 不动。
- `TestLastActiveNullFallback`：`last_active:null` 时用 `last_started_at` 判定（刚启动未活动的 compute 也能按时挂起，但启动后宽限 suspend_after 秒）。
- `TestReconcileRunningButPodGone`：state=running、runtime 报 pod 不存在 → 直接置 suspended。
- `TestSweepSkipsSuspending`：状态 CAS running→suspending 失败（已被并发改走）→ 跳过不报错。

- [ ] **Step 2: 确认失败 → Step 3: 实现**

```go
type Suspender struct {
    Store    *state.Store
    Runtime  compute.Runtime
    StatusURL func(addr string) string // addr=ip:55433 → http://ip:3080/status
    TerminateURL func(addr string) string
}
func (s *Suspender) Sweep(ctx context.Context) error // 单轮；main 里 ticker 驱动
```
Sweep 流程：`ListEndpointsByState("running")` → 逐个：Status 不存在→SetEndpointSuspended；GET /status 拿 last_active（解析 RFC3339，null fallback last_started_at）→ 空闲超 suspend_after → CAS running→suspending → POST `/terminate?mode=fast`（错误仅记日志，pod 删除兜底）→ Runtime.Stop → SetEndpointSuspended。

- [ ] **Step 4: 测试通过，提交** `git commit -m "feat: idle suspend scheduler with reconcile"`

### Task 16: M2 端到端验收（完整 serverless 闭环）

**Files:**
- Create: `tests/integration/m2_suspend_test.go`

- [ ] **Step 1: 写测试**

为测试把 endpoint 的 `suspend_after_seconds` 直接 UPDATE 为 15（或北向 API 支持传入）：
1. 建项目 → pgx 经 proxy 连接（冷唤醒）→ 写入数据 → 断开。
2. 轮询 k8s（最长 2 分钟）：`compute-<ep>` pod 消失，store 状态=suspended。
3. 再次 pgx 经 proxy 连接 → 自动唤醒 → 数据完好。
4. （执行期验证清单第 5 条）若 compute 的 last_active 不按预期更新导致永不挂起/过早挂起，在此实测并调整判定逻辑（例如改用 `/metrics` 中的活跃连接数辅助），回写 Task 15。

- [ ] **Step 2: 跑通提交** `git commit -m "test: M2 e2e — full serverless loop: wake, idle suspend, re-wake"`

---

## Phase 3（M3）：branching + 北向完善 + 集成测试套件

### Task 17: branching API

**Files:**
- Modify: `internal/api/api.go`, `internal/api/api_test.go`

- [ ] **Step 1: 写失败测试**

- `TestCreateBranch`：`POST /v1/projects/{id}/branches` body `{"name":"preview"}` → 201 含新 branch_id/endpoint_id/connection_uri（同项目 role，密码不重复返回——返回 `password:null` 并注明复用项目角色密码）；假 pageserver 收到带 `ancestor_timeline_id` 的 timeline POST（ancestor=默认分支 timeline）。
- `TestCreateBranchFromBranch`：body 带 `parent_branch_id` → ancestor 正确。
- `TestDeleteBranch`：→ compute Stop、pageserver DeleteTimeline、行删除；默认分支拒绝删除（400）。
- `TestDeleteProject`：→ 全部 endpoint Stop、DeleteTenant、级联删除。

- [ ] **Step 2: 确认失败 → Step 3: 实现 → Step 4: 测试通过，提交**

`git commit -m "feat: branching api + delete flows"`

### Task 18: 用量查询

**Files:**
- Modify: `internal/api/api.go`, `internal/api/api_test.go`

- [ ] **Step 1: 测试**：`GET /v1/projects/{id}/usage` → `{"branches":[{"branch_id":"…","logical_size_bytes":N}],"total_logical_size_bytes":N}`（假 pageserver 的 timeline detail 返回 current_logical_size）。
- [ ] **Step 2: 实现并提交** `git commit -m "feat: usage endpoint"`

### Task 19: M3 集成测试套件（验收主体）

**Files:**
- Create: `tests/integration/m3_branch_test.go`, `tests/integration/m3_chaos_test.go`

- [ ] **Step 1: branch 隔离性 e2e（m3_branch_test.go）**

1. 建项目 → 连接写入 `t1: 100 行` → 建分支 preview。
2. 连接分支 endpoint（SNI 用分支的 ep id，冷唤醒）→ `SELECT count(*) FROM t1`==100（COW 继承）。
3. 分支上 `INSERT 50 行` → 分支 count==150，主分支仍 ==100。**branching 核心验收。**
4. 删除分支 → pod 消失、timeline 删除；主分支不受影响。

- [ ] **Step 2: 异常路径 e2e（m3_chaos_test.go）**

- 并发唤醒：suspended endpoint，10 个并发 pgx 连接 → 全部成功，k8s 中 compute pod 只有 1 个。
- 挂起竞态：把 suspend_after 调到 15s，循环"断开→挂起窗口内立即重连"10 次 → 每次都成功。
- pageserver 重启恢复：`kubectl rollout restart statefulset/pageserver` → 等 Ready → 既有 endpoint 重新唤醒后数据完好。

- [ ] **Step 3: 跑通提交** `git commit -m "test: M3 e2e — branch isolation, concurrent wake, suspend race, pageserver restart"`

### Task 20: README 与最终验收

**Files:**
- Create/Modify: `README.md`

- [ ] **Step 1: README**：架构图（spec 摘录）、快速开始（Task 1 环境 → make deploy-storage → make certs → make deploy-cp → make forward → curl 建项目 → psql 连接示例含 sslrootcert）、Makefile 目标表、与 spec/计划文档的链接。
- [ ] **Step 2: 最终验收跑全套**

```bash
go test ./internal/... && make integration   # 全绿
```
对照 spec 第 4 节验收清单逐项打勾：建项目 ✓ 连接唤醒 ✓ 读写 ✓ 自动挂起 ✓ 再唤醒数据完好 ✓ 秒级分支 ✓ 分支隔离 ✓。

- [ ] **Step 3: 提交** `git commit -m "docs: readme + M1-M3 acceptance"`

---

## 自查记录

- **Spec 覆盖**：spec §1 三能力→Task 14/16（scale-to-zero）、17/19（branching）、10（存算分离）；§3.2 四职责→Task 9（北向）、12（契约）、8/13（生命周期）、15（挂起）；§4 本地环境→Task 1/3/11；§6 测试策略→各任务 TDD + Task 10/14/16/19；§7 M1/M2/M3 验收→Task 10/16/19-20。M4（AWS）不在本计划范围。
- **类型一致性**：`compute.ComputeConfig` 贯穿 Task 7/8/13；`state.Store` 方法签名 Task 4 定义、6-18 引用；`Waker` 接口 Task 12 定义、13 实现。
- **无占位符**：所有代码块给出实际内容；五个版本漂移风险点集中在「执行期验证清单」，每条都有判定方法与兜底动作。
