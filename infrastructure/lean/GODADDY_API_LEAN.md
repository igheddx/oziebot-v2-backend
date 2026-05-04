# Point `api.oziebot.com` at the lean host

Do this **only after**:

1. Lean stack runs on the VPS with **`docker-compose.lean.edge.yml`** (Caddy) and curls clean from the Internet.
2. You have **`pg_dump`/restore plan** — see `README_LEAN_MODE.md`. The lean Postgres container must contain the same users/data as prod **or** you use **`LEAN_DATABASE_TARGET=rds`** and open RDS SG to **only this host’s IPv4**.
3. You are ready for **traffic to leave ECS** (`aws-scale-ecs-to-zero`) **after DNS propagates** (or tolerate brief overlap carefully).

---

## 1. Pick host networking

### Option A — Lightsail (simple static IP)

- Create instance (**Ubuntu 22.04+**, ≥ **4 GB** RAM recommended; **8 GB** safer).
- **Attach static IPv4** in Lightsail networking.
- In **Lightsail firewall** (or Ubuntu `ufw`): allow **TCP 80, 443** (and optionally **22** SSH from your IP only).

### Option B — EC2 (outside original VPC)

- Elastic IP associate to instance.
- **Security group** inbound: **80, 443** from `0.0.0.0/0`, **22** from your IP.

---

## 2. Deploy lean + Caddy on the host

On the VPS (SSH):

```bash
sudo apt-get update && sudo apt-get install -y docker.io docker-compose-plugin git
sudo usermod -aG docker "$USER"   # re-login afterward
```

Clone or rsync repo, then:

```bash
cd /path/to/oziebot
# .env.lean must exist on the server (scp from workstation; never commit)
docker compose \
  -f docker-compose.lean.yml \
  -f docker-compose.lean.edge.yml \
  --env-file .env.lean up -d --build
curl -sf http://127.0.0.1/v1/ready   # fails (no port) — instead:
curl -sf https://api.oziebot.com/v1/ready   # after DNS switch; or temporarily curl host IP:
curl -sf -H 'Host: api.oziebot.com' http://YOUR_HOST_PUBLIC_IP/v1/ready
```

Caddy listens on **:80/:443**. It will request certs only when **`api.oziebot.com` resolves to this IP**.

---

## 3. GoDaddy DNS (no Route 53)

1. Sign in → **Domains** → **oziebot.com** → **DNS**.
2. **Remove** conflicting records for **`api`**:
   - Delete old **ALIAS**/**CNAME** pointing at the ELB (**`*.elb.amazonaws.com`**).
   - Remove duplicate **`api`** A records.
3. **Add** record:
   - **Type**: `A`
   - **Host**: `api`
   - **Points to**: your **Lightsail static IP / EC2 Elastic IP** (IPv4)
   - **TTL**: **600** seconds (or lower for quicker rollback during cutover)
4. Wait for propagation (**minutes to hours**). Check:  
   `dig +short api.oziebot.com` → must return your VPS IP.

**IPv6**: If GoDaddy AAAA existed for `api`, remove unless you terminate TLS correctly on IPv6 (Caddy can be extended).

---

## 4. Frontend / CORS

`CORS_ORIGINS` in `.env.lean` should include **`https://app.oziebot.com`** (already typical). After cutover the browser still talks to **`https://api.oziebot.com`** → no frontend URL change unless you used a temporary hostname.

---

## 5. Cut AWS cost (after HTTPS is stable)

When `curl https://api.oziebot.com/v1/ready` → **200** from multiple networks:

```bash
export AWS_PROFILE=oziebot AWS_REGION=us-east-1
./infrastructure/lean/aws-scale-ecs-to-zero.sh
```

**ALB**: still billed until **deleted manually** once no traffic needs it (**manual confirmation**).  
**RDS / ElastiCache**: move to **`LEAN_DATABASE_TARGET=rds`** temporarily or migrate to Postgres container + **snapshot** before stopping managed DB.

---

## 6. Rollback DNS

Reverse step 3: **`api`** A or CNAME → back to **`oziebot-api-alb-433993103.us-east-1.elb.amazonaws.com`** (or whatever you used), then `./infrastructure/lean/aws-scale-ecs-restore.sh`.
