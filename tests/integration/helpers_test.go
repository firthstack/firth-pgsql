//go:build integration

// Package integration holds end-to-end tests against a running firth-pgsql
// deployment on the local k8s cluster (OrbStack). Prerequisites:
//
//	make deploy-storage deploy-cp   # all pods Running
//	make certs && kubectl apply -f deploy/k8s/70-proxy.yaml   # for proxy tests
//
// The tests create their own port-forwards via kubectl.
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const namespace = "firth-pgsql"

// portForward starts kubectl port-forward and waits until the local port
// accepts connections. Returns a cleanup func.
func portForward(t *testing.T, target string, localPort, remotePort int) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "kubectl", "-n", namespace, "port-forward", target,
		fmt.Sprintf("%d:%d", localPort, remotePort))
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("port-forward %s: %v", target, err)
	}
	t.Cleanup(func() {
		cancel()
		_ = cmd.Wait()
	})
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", localPort), time.Second)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("port-forward %s never became ready", target)
}

type project struct {
	ProjectID     string `json:"project_id"`
	BranchID      string `json:"branch_id"`
	EndpointID    string `json:"endpoint_id"`
	Role          string `json:"role"`
	Password      string `json:"password"`
	Host          string `json:"host"`
	Database      string `json:"database"`
	ConnectionURI string `json:"connection_uri"`
}

type controlPlane struct {
	t    *testing.T
	base string
}

// newControlPlane port-forwards the control plane service once per test.
func newControlPlane(t *testing.T, localPort int) *controlPlane {
	t.Helper()
	portForward(t, "svc/controlplane", localPort, 8080)
	return &controlPlane{t: t, base: fmt.Sprintf("http://127.0.0.1:%d", localPort)}
}

func (c *controlPlane) doJSON(method, path string, body string, out any) (int, string) {
	c.t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, c.base+path, rd)
	if err != nil {
		c.t.Fatal(err)
	}
	hc := http.Client{Timeout: 3 * time.Minute}
	resp, err := hc.Do(req)
	if err != nil {
		c.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if out != nil {
		_ = json.Unmarshal(raw, out)
	}
	return resp.StatusCode, string(raw)
}

func (c *controlPlane) createProject(name string) project {
	c.t.Helper()
	var p project
	code, raw := c.doJSON(http.MethodPost, "/v1/projects", fmt.Sprintf(`{"name":%q}`, name), &p)
	if code != http.StatusCreated {
		c.t.Fatalf("create project: %d %s", code, raw)
	}
	return p
}

func (c *controlPlane) debugStart(endpointID string) string {
	c.t.Helper()
	var out struct {
		Address string `json:"address"`
		Error   string `json:"error"`
	}
	code, raw := c.doJSON(http.MethodPost, "/v1/debug/endpoints/"+endpointID+"/start", "", &out)
	if code != http.StatusOK {
		c.t.Fatalf("debug start: %d %s", code, raw)
	}
	return out.Address
}

func (c *controlPlane) debugStop(endpointID string) {
	c.t.Helper()
	code, raw := c.doJSON(http.MethodPost, "/v1/debug/endpoints/"+endpointID+"/stop", "", nil)
	if code != http.StatusOK {
		c.t.Fatalf("debug stop: %d %s", code, raw)
	}
}

func kubectl(t *testing.T, args ...string) (string, error) {
	t.Helper()
	out, err := exec.Command("kubectl", append([]string{"-n", namespace}, args...)...).CombinedOutput()
	return string(out), err
}

func computePodExists(t *testing.T, endpointID string) bool {
	t.Helper()
	_, err := kubectl(t, "get", "pod", "compute-"+endpointID)
	return err == nil
}
