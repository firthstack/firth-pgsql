// Package compute builds compute_ctl configs and manages compute pods.
//
// The JSON shapes follow neondatabase/neon's ComputeConfig/ComputeSpec
// (libs/compute_api/src/spec.rs) as consumed by compute_ctl --config.
package compute

// ComputeConfig is the file passed to compute_ctl via --config: the spec
// wrapped together with compute_ctl's own config (jwks; empty is fine since
// we run compute_ctl with --dev which skips API authorization).
type ComputeConfig struct {
	Spec             ComputeSpec      `json:"spec"`
	ComputeCtlConfig ComputeCtlConfig `json:"compute_ctl_config"`
}

type ComputeCtlConfig struct {
	Jwks Jwks `json:"jwks"`
}

type Jwks struct {
	Keys []any `json:"keys"`
}

type ComputeSpec struct {
	FormatVersion         float64  `json:"format_version"`
	SuspendTimeoutSeconds int64    `json:"suspend_timeout_seconds"`
	Cluster               Cluster  `json:"cluster"`
	DeltaOperations       []any    `json:"delta_operations"`
	TenantID              string   `json:"tenant_id"`
	TimelineID            string   `json:"timeline_id"`
	PageserverConnstring  string   `json:"pageserver_connstring"`
	SafekeeperConnstrings []string `json:"safekeeper_connstrings"`
	Mode                  string   `json:"mode"`
	SkipPgCatalogUpdates  bool     `json:"skip_pg_catalog_updates"`
}

type Cluster struct {
	ClusterID string     `json:"cluster_id"`
	Name      string     `json:"name"`
	State     string     `json:"state"`
	Roles     []Role     `json:"roles"`
	Databases []Database `json:"databases"`
	Settings  []Setting  `json:"settings"`
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

type Setting struct {
	Name    string `json:"name"`
	Value   string `json:"value"`
	Vartype string `json:"vartype"`
}

// cloudAdminMD5 is md5("cloud_admin"+"cloud_admin") — the compute superuser
// password used by the control plane and upstream's docker-compose alike.
const cloudAdminMD5 = "b093c0d3b281ba6da1eacc608620abd8"

// ComputePort is the in-pod postgres port (kept identical to upstream's
// docker-compose to minimize divergence from the pinned images).
const ComputePort = 55433

type SpecParams struct {
	TenantID             string
	TimelineID           string
	RoleName             string
	RoleVerifier         string // SCRAM verifier, used as encrypted_password verbatim
	DatabaseName         string
	PageserverConnstring string
	Safekeepers          []string
}

func BuildComputeConfig(p SpecParams) ComputeConfig {
	admin := cloudAdminMD5
	appSecret := p.RoleVerifier
	return ComputeConfig{
		Spec: ComputeSpec{
			FormatVersion:         1.0,
			SuspendTimeoutSeconds: -1, // suspension is driven externally by our scheduler
			TenantID:              p.TenantID,
			TimelineID:            p.TimelineID,
			PageserverConnstring:  p.PageserverConnstring,
			SafekeeperConnstrings: p.Safekeepers,
			Mode:                  "Primary",
			DeltaOperations:       []any{},
			Cluster: Cluster{
				ClusterID: "fly-pgsql",
				Name:      "fly-pgsql",
				State:     "restarted",
				Roles: []Role{
					{Name: "cloud_admin", EncryptedPassword: &admin},
					{Name: p.RoleName, EncryptedPassword: &appSecret},
				},
				Databases: []Database{
					{Name: p.DatabaseName, Owner: p.RoleName},
				},
				Settings: defaultSettings(),
			},
		},
		ComputeCtlConfig: ComputeCtlConfig{Jwks: Jwks{Keys: []any{}}},
	}
}

func defaultSettings() []Setting {
	return []Setting{
		{"fsync", "off", "bool"},
		{"wal_level", "logical", "enum"},
		{"wal_log_hints", "on", "bool"},
		{"log_connections", "on", "bool"},
		{"port", "55433", "integer"},
		{"shared_buffers", "128MB", "string"},
		{"max_connections", "100", "integer"},
		{"listen_addresses", "0.0.0.0", "string"},
		{"max_wal_senders", "10", "integer"},
		{"max_replication_slots", "10", "integer"},
		{"wal_sender_timeout", "5s", "string"},
		{"wal_keep_size", "0", "integer"},
		{"password_encryption", "scram-sha-256", "enum"},
		{"restart_after_crash", "off", "bool"},
		{"synchronous_standby_names", "walproposer", "string"},
		{"shared_preload_libraries", "neon,pg_stat_statements", "string"},
		{"max_replication_write_lag", "500MB", "string"},
		{"max_replication_flush_lag", "10GB", "string"},
	}
}
