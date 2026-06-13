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
