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
