# Oziebot lean mode (70–90% AWS cost reduction)

This document describes **validation-phase** hosting: **one VPS (Lightsail or small EC2)** running **Docker Compose** with Postgres + Redis + all backend workers, while the **frontend stays on S3 + CloudFront** when already deployed. **No trading code changes.** ECS/Fargate definitions remain in-repo for a future scale-up.

| Area | Source of truth |
|------|-----------------|
| Lean compose | `docker-compose.lean.yml`, `.env.lean.example`; **TLS:** `docker-compose.lean.edge.yml` + `infrastructure/lean/Caddyfile` |
| DNS / cutover | **`infrastructure/lean/GODADDY_API_LEAN.md`** |
| **IAM + Lightsail + who-does-what** | **`infrastructure/lean/COST_CUT_PLAYBOOK.md`** |
| Scripts | `infrastructure/lean/*.sh` |
| Prod reference (ECS) | `infrastructure/aws/backend/service-map.yml`, `env-matrix.yml` |

---

## 1. Current AWS dependencies (from repo)

| Resource | Role | Lean replacement |
|---------|------|------------------|
| ECS Fargate | api, strategy-engine, risk-engine, execution-engine, alerts-worker, market-data-ingestor | **Compose on one host** |
| ALB | HTTPS → API tasks | **GoDaddy A/AAAA → host public IP** (TLS via reverse proxy on host—see below) |
| RDS PostgreSQL | Primary DB | **Postgres container** *or* optional tiny RDS |
| ElastiCache Redis | Queues, cache, runtime heartbeats in prod | **Redis container** (`REDIS_URL=redis://redis:6379/0`) |
| CloudFront + S3 | `app.oziebot.com` | **Keep** (typically a few dollars) |
| ECR | Images | **Keep** (storage cheap; do not delete images) |
| Secrets Manager | Prod secrets | **Keep**; lean uses `.env.lean` on host only |
| CloudWatch Logs | `/ecs/...` | **Short retention** or N/A when tasks stopped |

**24/7 in lean mode:** the single host + (optional) CloudFront/S3. **Not** required: Fargate tasks, ALB data plane, ElastiCache, large RDS—once cut over.

**Expensive when “always on” at prod scale:** Fargate aggregate CPU/RAM, **NAT Gateway**, **ALB**, **RDS + ElastiCache** instance hours, log ingestion.

---

## 2. Cost-reduction options

Rough **US East** order-of-magnitude (varies by sizing, NAT, storage, and data transfer). Use Cost Explorer for your account.

### Option A — Pause expensive AWS only

**Idea:** Scale ECS services **desired count → 0**, **snapshot RDS**, optionally **stop** RDS instance (not deleted), delete **NAT** only if nothing else needs it, scale ElastiCache nodes to 0 / delete replica set **only after confirmation**.

| | |
|--|--|
| **Expected monthly** | Often **\$80–\$350+** left if ALB + NAT + RDS storage/snapshots + secrets + partial networking remain |
| **Pros** | Fast; no migration compose host |
| **Cons** | ALB/NAT/RDS **idle cost** can still dominate; easy to forget orphans |
| **Risk** | Low if you only scale to 0 and keep snapshots |
| **Rollback** | `infrastructure/lean/aws-scale-ecs-restore.sh`; restore RDS from snapshot if stopped |

### Option B — **Recommended:** One Lightsail / small EC2 + Docker Compose (this repo)

| | |
|--|--|
| **Expected monthly** | **~\$25–\$80**: Lightsail **4–8 GB** (\$24–\$44) + **S3/CloudFront** + **Secrets** (\$~few) + **ECR** storage; optional tiny RDS adds **\$15+** |
| **Pros** | Largest recurring savings; same code paths; Redis/Postgres local; reversible |
| **Cons** | You operate patches/backups/Disk; single point of failure; TLS/firewall on you |
| **Risk** | Medium ops burden; secure the host |
| **Rollback** | Point `api.oziebot.com` back to ALB; restore ECS; re-point `DATABASE_URL` / `REDIS_URL` to managed |

### Option C — Stay on ECS, shrink footprint

**Idea:** Lower **task CPU/memory**, **min = max = 1** task per service where safe, **shorter** CloudWatch retention, reduce **market-data**/`api` log noise, **spot** (if supported in your setup) — *Fargate spot is region/account specific*.

| | |
|--|--|
| **Expected monthly** | **~\$30–60%** of current compute/log slice; **ALB + NAT + RDS + Redis** still present |
| **Pros** | Keeps managed ECS story |
| **Cons** | Still expensive vs single VM; complexity remains |
| **Risk** | Low–medium (OOM if cut too deep) |
| **Rollback** | Bump task sizes in console/task defs |

**Recommendation:** **Option B** for “validation phase”; keep **Option C** tweaks in pocket for a future “small prod ECS.”

---

## 3. Lean implementation (files)

| File | Purpose |
|------|---------|
| `docker-compose.lean.edge.yml` | **Caddy** on :80/:443 → `api:8000` (Let’s Encrypt for `api.oziebot.com`) |
| `infrastructure/lean/Caddyfile` | TLS hostname + `reverse_proxy` |
| **`infrastructure/lean/GODADDY_API_LEAN.md`** | **GoDaddy A record** + firewall + ECS scale-after-cutover |

**HTTPS on the VPS:** Merge edge file when DNS points at the host:

```bash
docker compose -f docker-compose.lean.yml -f docker-compose.lean.edge.yml --env-file .env.lean up -d --build
```

| File | Purpose |
|------|---------|
| `docker-compose.lean.yml` | Postgres, Redis, API, all workers; log rotation; memory hints |
| `.env.lean.example` | Copy → `.env.lean` (gitignored) |
| `infrastructure/lean/bootstrap-lean-env-from-aws.sh` | Build **`.env.lean`** from Secrets Manager (hex DB password for local Postgres); `LEAN_BOOTSTRAP_OVERWRITE=1` to replace |
| `infrastructure/lean/deploy-lean-host.sh` | SSH + `compose up` (optional `--rsync`, `LEAN_USE_EDGE=1` for Caddy TLS) |
| `infrastructure/lean/lean-services.sh` | `start` / `stop` / `restart` / `ps` / `down` |
| `infrastructure/lean/healthcheck-lean.sh` | API `/v1/ready`, Redis, Postgres |
| `infrastructure/lean/pg-backup.sh` | `pg_dump` → `backups/lean-pg/` |
| `infrastructure/lean/pg-restore.sh` | Restore from `.sql.gz` (**destructive**) |
| `infrastructure/lean/aws-scale-ecs-to-zero.sh` | Stop Fargate tasks |
| `infrastructure/lean/aws-scale-ecs-restore.sh` | Set desired count back to 1 |
| `infrastructure/lean/aws-cloudwatch-retention.sh` | `/ecs/*` retention (default 7d) |

**Host sizing:** **≥ 4 GB RAM** minimum (**8 GB** comfortable) for seven Python services + Postgres + Redis. Lightsail **8 GB** or **EC2 t3.large**/similar.

**API bind:** Default **`LEAN_API_BIND=127.0.0.1`** (curl from host for debug). Public traffic uses **Caddy → `api:8000`** inside Docker; avoid exposing **8000** on `0.0.0.0` unless required.

---

## 4. Database plan

**`pg-backup.sh` / `pg-restore.sh`** use database/user name **`oziebot`** by default. If you rename `POSTGRES_DB` / `POSTGRES_USER` in `.env.lean`, edit those scripts accordingly.

| Choice | When |
|--------|------|
| **Postgres container** (default) | Cheapest; snapshot host volume / `pg-backup.sh` |
| **Tiny RDS** (db.t4g.micro) | If you want AWS-managed durability without full prod spend |

**RDS logical dump from laptop:** Often fails (`connection refused`/private RFC1918 endpoint) unless RDS is reachable on the Internet or you use VPC access (SSH bastion / SSM tunnel / ECS one-off container with PostgreSQL client). Prefer **RDS snapshot** (already taken) plus restore into container when you have VPC access—or use `pg_restore` from an in-VPC runner.

**Before touching RDS:**

1. **Manual RDS snapshot** (console or CLI).
2. Export: `pg_dump` connection string compatible with `pg_dump` (use `postgresql://...`, **not** `+psycopg`):

```bash
# Example — replace host/user/pass; use sslmode=require as in your RDS
pg_dump "postgresql://USER:PASS@RDS_HOST:5432/oziebot?sslmode=require" \
  --no-owner --format=plain | gzip > oziebot-rds-export.sql.gz
```

3. Stand up lean stack; run `pg-restore.sh oziebot-rds-export.sql.gz` **or** `\i` via `psql` after DB create.

4. **Do not delete** RDS until lean DB verified and **snapshot retained**.

**`DATABASE_URL` in `.env.lean` for compose networking:**

`postgresql+psycopg://oziebot:YOUR_PASSWORD@postgres:5432/oziebot`

---

## 5. Redis plan

- **`REDIS_URL=redis://redis:6379/0`** inside Compose (same **queue semantics** as prod; ElastiCache removed from path).
- **Do not** set `OBSERVABILITY_S3_*` unless you intentionally want S3-backed heartbeats; lean uses **Redis-only** runtime snapshots like local dev.

---

## 6. ECS / Fargate: scale to zero (CLI)

**After** lean API answers at new endpoint and DNS **or** during a maintenance window:

```bash
export AWS_PROFILE=oziebot
export AWS_REGION=us-east-1
./infrastructure/lean/aws-scale-ecs-to-zero.sh
```

**Rollback (starts 1 task each — adjust counts in console if you use >1 API tasks):**

```bash
./infrastructure/lean/aws-scale-ecs-restore.sh
```

**Not deleted:** task definitions, ECR images, clusters.

---

## 7. ALB, DNS, CloudFront

| Component | Lean approach |
|-----------|----------------|
| **ALB** | Tasks at 0 → unhealthy; **point DNS away first** or expect errors. **ALB still bills** until deleted—**manual confirmation** before delete. |
| **`api.oziebot.com`** (GoDaddy) | **Step-by-step:** **`infrastructure/lean/GODADDY_API_LEAN.md`** |
| **`app.oziebot.com`** | **Keep** CloudFront + S3; **`CORS_ORIGINS`** must include `https://app.oziebot.com` |

**ACM on ALB** is irrelevant once DNS points at lean host; certs are obtained by **Caddy** instead.

---

## 8. CloudWatch log cost

Set retention (example **7 days**) for `/ecs/` groups:

```bash
export AWS_PROFILE=oziebot
export AWS_REGION=us-east-1
export RETENTION_DAYS=7
./infrastructure/lean/aws-cloudwatch-retention.sh
```

Narrow `LOG_GROUP_PREFIX` if you share accounts. Lean mode: most logs live in **Docker** (`json-file` capped in compose).

---

## 9. Secrets (lean)

| Variable | Required | Notes |
|----------|----------|--------|
| `DATABASE_URL` | Yes | Compose `postgres` hostname |
| `REDIS_URL` | Yes | `redis://redis:6379/0` |
| `JWT_SECRET` | Yes | Strong random |
| `EXCHANGE_CREDENTIALS_ENCRYPTION_KEY` | Yes for live credentials in DB | Same as prod if migrating DB |
| `COINBASE_*` | Via app/DB | API base for REST |
| Stripe vars | If billing used | |
| `SLACK_WEBHOOK_URL` / `TELEGRAM_BOT_TOKEN` / `SMS_WEBHOOK_URL` | If alerts used | |

**Never commit** `.env.lean`. Pull from Secrets Manager to a secure local file when populating the host.

---

## 10. Safety & rollback checklist

1. **RDS snapshot** (manual).
2. **Pg_dump** to durable storage.
3. Bring up **lean** stack; **migrate** DB if needed; run **`healthcheck-lean.sh` on the host**.
4. Point **`api.oziebot.com`** to new IP; verify **HTTPS** and **`/v1/ready`**.
5. Update **frontend** env if API URL changed; redeploy **CloudFront** origin if applicable.
6. **Scale ECS to 0**; monitor cost for **ALB/NAT**—plan delete or keep.
7. **Rollback:** DNS back to ALB → **`aws-scale-ecs-restore.sh`** → verify RDS/ElastiCache endpoints in task defs/env.

---

## 11. Cost-saving checklist (ordered)

- [ ] Snapshot RDS; export dump.
- [ ] Build `.env.lean` from `.env.lean.example` + prod secrets.
- [ ] `docker compose -f docker-compose.lean.yml --env-file .env.lean up -d --build` on host.
- [ ] Healthchecks green; smoke-test login + paper trading.
- [ ] GoDaddy **`api`** → new host IP; TLS in place.
- [ ] `./infrastructure/lean/aws-scale-ecs-to-zero.sh`
- [ ] **`aws-cloudwatch-retention.sh`** (7d) or delete unused log groups (**manual**).
- [ ] Cost Explorer: confirm **NAT/ALB/ElastiCache/RDS** — **stop or delete only after manual confirmation**.
- [ ] After **30 days** stable: delete **unused** NAT/EIP/ALB (**manual**), keep **ECR** and **snapshots** as needed.

---

## 12. Resources: stop now vs keep vs manual confirmation

| Action now (typical) | Keep | Manual confirmation before delete |
|----------------------|------|-----------------------------------|
| ECS desired → **0** | ECR, task defs, cluster | **ALB** (still \$) |
| Shorten log retention | S3 **frontend** bucket | **RDS** (snapshot first) |
| Lean host runs DB/Redis | Secrets Manager keys | **ElastiCache** cluster |
| | CloudFront distribution | **NAT Gateway** (often \$30+/mo idle) |

---

**`pg-backup.sh` / `pg-restore.sh`** default to database/user **`oziebot`**. If you change `POSTGRES_DB` / `POSTGRES_USER` in `.env.lean`, adjust those scripts or pass dumps manually.

## 13. Clean up “unused” after lean is stable

**Only after** end-to-end validation (paper/live as you use it):

1. Reconcile **Cost Explorer** + **Resource Groups** for stray ENIs/EIPs.
2. **Delete ALB** (and target groups/listeners) if DNS no longer points to it — **confirmation**.
3. **Delete ElastiCache** replication group if nothing points to it — **confirmation**.
4. **Stop or delete RDS** only with **final snapshot** and **`DATABASE_URL` migrated** — **confirmation**.
5. **Remove NAT gateways** only if no AWS workload needs private subnet egress — **confirmation** (often breaks other stacks).

---

## Quick start (local or single host)

```bash
cp .env.lean.example .env.lean
# edit .env.lean — strong secrets, CORS, passwords

docker compose -f docker-compose.lean.yml --env-file .env.lean up -d --build
./infrastructure/lean/healthcheck-lean.sh
```

Remote deploy:

```bash
export LEAN_SSH=ubuntu@YOUR_IP
export LEAN_REPO_PATH=/home/ubuntu/oziebot
./infrastructure/lean/deploy-lean-host.sh --rsync
# ensure .env.lean is on server (scp once; do not rsync if excluded)
```

---

*This is infrastructure guidance, not financial advice. Validate backups before any destructive step.*
