package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/insforge/firth-pgsql/internal/api"
	"github.com/insforge/firth-pgsql/internal/compute"
	"github.com/insforge/firth-pgsql/internal/neonclient"
	"github.com/insforge/firth-pgsql/internal/proxycontract"
	"github.com/insforge/firth-pgsql/internal/state"
	"github.com/insforge/firth-pgsql/internal/suspend"
	"github.com/insforge/firth-pgsql/internal/wake"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func k8sClient() (kubernetes.Interface, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		// Local development: fall back to kubeconfig.
		rules := clientcmd.NewDefaultClientConfigLoadingRules()
		cfg, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, nil).ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("no in-cluster config and no kubeconfig: %w", err)
		}
	}
	return kubernetes.NewForConfig(cfg)
}

func main() {
	ctx := context.Background()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	dbURL := env("DATABASE_URL", "postgres://firthpgsql:firthpgsql@statedb:5432/firthpgsql")
	namespace := env("NAMESPACE", "firth-pgsql")
	pageserverURL := env("PAGESERVER_URL", "http://pageserver:9898")
	pageserverConnstring := env("PAGESERVER_CONNSTRING", "host=pageserver port=6400")
	safekeepers := strings.Split(env("SAFEKEEPERS",
		"safekeeper-0.safekeeper:5454,safekeeper-1.safekeeper:5454,safekeeper-2.safekeeper:5454"), ",")
	computeImage := env("COMPUTE_IMAGE", "ghcr.io/neondatabase/compute-node-v17:release-compute-9073")
	domain := env("DOMAIN", "db.127-0-0-1.sslip.io")
	listen := env("LISTEN", ":8080")
	authToken := env("CONTROL_PLANE_AUTH_TOKEN", "")
	enableDebug := env("ENABLE_DEBUG_ENDPOINTS", "") == "true"
	suspendInterval, err := time.ParseDuration(env("SUSPEND_CHECK_INTERVAL", "30s"))
	if err != nil {
		slog.Error("bad SUSPEND_CHECK_INTERVAL", "err", err)
		os.Exit(1)
	}

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		slog.Error("connect state db", "err", err)
		os.Exit(1)
	}
	// State db may still be booting when we start; retry migration briefly.
	for i := 0; ; i++ {
		if err = state.Migrate(ctx, pool); err == nil {
			break
		}
		if i >= 30 {
			slog.Error("migrate", "err", err)
			os.Exit(1)
		}
		time.Sleep(2 * time.Second)
	}

	kube, err := k8sClient()
	if err != nil {
		slog.Error("k8s client", "err", err)
		os.Exit(1)
	}

	store := state.New(pool)
	pageserver := neonclient.NewPageserver(pageserverURL)
	// Derive safekeeper HTTP endpoints (port 7676) from their pg connstrings
	// (host:5454), used to read the committed LSN when branching.
	safekeeperHTTP := make([]string, 0, len(safekeepers))
	for _, sk := range safekeepers {
		host, _, _ := strings.Cut(sk, ":")
		safekeeperHTTP = append(safekeeperHTTP, "http://"+host+":7676")
	}
	safekeeper := neonclient.NewSafekeeper(safekeeperHTTP)
	runtime := compute.NewK8sRuntime(kube, namespace, computeImage)

	specBuilder := func(ctx context.Context, endpointID string) (compute.ComputeConfig, error) {
		ac, err := store.GetAccessControl(ctx, endpointID)
		if err != nil {
			return compute.ComputeConfig{}, err
		}
		p, err := store.GetProjectByID(ctx, ac.ProjectID)
		if err != nil {
			return compute.ComputeConfig{}, err
		}
		b, err := store.GetBranchByID(ctx, ac.BranchID)
		if err != nil {
			return compute.ComputeConfig{}, err
		}
		return compute.BuildComputeConfig(compute.SpecParams{
			TenantID:             p.TenantID,
			TimelineID:           b.TimelineID,
			RoleName:             p.RoleName,
			RoleVerifier:         p.RoleVerifier,
			DatabaseName:         "appdb",
			PageserverConnstring: pageserverConnstring,
			Safekeepers:          safekeepers,
		}), nil
	}

	waker := &wake.Waker{
		Store:       store,
		Runtime:     runtime,
		SpecBuilder: specBuilder,
	}

	apiServer := &api.Server{
		Store:      store,
		Pageserver: pageserver,
		Safekeeper: safekeeper,
		Runtime:    runtime,
		Waker:      waker,
		Cfg: api.Config{
			Domain:               domain,
			ProxyPort:            5432,
			PageserverConnstring: pageserverConnstring,
			Safekeepers:          safekeepers,
			AuthToken:            authToken,
			EnableDebug:          enableDebug,
		},
	}

	mux := apiServer.Routes()
	(&proxycontract.Handlers{Store: store, Waker: waker}).Register(mux)

	suspender := &suspend.Suspender{Store: store, Runtime: runtime}
	go func() {
		ticker := time.NewTicker(suspendInterval)
		defer ticker.Stop()
		for range ticker.C {
			if err := suspender.Sweep(ctx); err != nil {
				slog.Error("suspend sweep", "err", err)
			}
		}
	}()

	slog.Info("controlplane listening", "addr", listen)
	if err := http.ListenAndServe(listen, mux); err != nil {
		slog.Error("serve", "err", err)
		os.Exit(1)
	}
}
