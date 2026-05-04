# Cost cut: what only you can do vs what the agent can run

## What is actually going on

- **This environment is not logged into your AWS account.** Anything “I” do is **`aws`/shell on your laptop** using **`AWS_PROFILE=...`**.
- **`cryptobotty-deploy` currently cannot call Lightsail** (`lightsail:GetStaticIps` returned `AccessDenied`), so **no Lightsail VM can be created or read** until you **grant IAM**.
- **GoDaddy** has **no AWS-style API** in this workspace: **changing `api.oziebot.com`** is something **you click** (or automate elsewhere).

So “take care of this” means: **you do one IAM + DNS step**, then the agent **can** run scripted Lightsail + Docker + ECS scale-down commands from your machine.

---

## Step 1 — You (once): widen IAM for Lightsail (console)

Sign in as an **account admin**, open **IAM → Users → cryptobotty-deploy → Add permissions**, and attach an **inline policy** (example below).

**Scope:** This is broad on **Lightsail only** inside your account. Tighten later if you split deploy vs infra users.

<details>
<summary>Example inline policy JSON (Lightsail + read EC2 EIP for troubleshooting)</summary>

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "LightsailOziebotProvision",
      "Effect": "Allow",
      "Action": [
        "lightsail:*"
      ],
      "Resource": "*"
    },
    {
      "Sid": "DescribeEC2ForEIPPlanning",
      "Effect": "Allow",
      "Action": [
        "ec2:DescribeAddresses",
        "ec2:DescribeInstances"
      ],
      "Resource": "*"
    }
  ]
}
```

For **least-privilege**, replace `lightsail:*` with only the verbs you want (minimum: `Get*`, `Create*`, `Allocate*`, `Attach*`, `Open*`, `Put*`, `Start*`, `Stop*`, `Delete*` for instances/static IPs/keys as needed). The broad version is the fastest way to unblock.

</details>

**Verify on your Mac:**

```bash
export AWS_PROFILE=oziebot AWS_REGION=us-east-1
aws lightsail get-bundles --region "$AWS_REGION" --query 'bundles[0].bundleId' --output text
```

If that returns a bundle id (not `AccessDenied`), **Step 2 is unblocked**.

---

## Step 2 — Agent / you: create Lightsail + static IP (CLI)

Replace names if you like. **Region** should match where you want the API (e.g. `us-east-1`).

```bash
export AWS_PROFILE=oziebot
export AWS_REGION=us-east-1

# Pick an AZ in that region (example)
AZ="${AWS_REGION}a"

# 1) SSH key in Lightsail (one-time). If you already have a Lightsail key pair named oziebot-lean, skip.
aws lightsail create-key-pair --key-pair-name oziebot-lean --region "$AWS_REGION" \
  --query 'privateKeyBase64' --output text | base64 -d > ~/.ssh/oziebot-lean.pem
chmod 600 ~/.ssh/oziebot-lean.pem

# 2) Instance (Ubuntu 22.04, 8 GB / 2 vCPU class — adjust bundle to your budget)
aws lightsail create-instances \
  --region "$AWS_REGION" \
  --instance-names oziebot-lean-api \
  --availability-zone "$AZ" \
  --blueprint-id ubuntu_22_04 \
  --bundle-id large_3_0 \
  --key-pair-name oziebot-lean

# Wait until running
aws lightsail get-instance --instance-name oziebot-lean-api --region "$AWS_REGION" \
  --query 'instance.state.name' --output text

# 3) Static IP (stable for GoDaddy A record)
aws lightsail allocate-static-ip --static-ip-name oziebot-lean-api-ip --region "$AWS_REGION"
aws lightsail attach-static-ip --static-ip-name oziebot-lean-api-ip --instance-name oziebot-lean-api --region "$AWS_REGION"

# 4) Show the public IP you will put in GoDaddy
aws lightsail get-static-ip --static-ip-name oziebot-lean-api-ip --region "$AWS_REGION" \
  --query 'staticIp.ipAddress' --output text
```

**Blueprint / bundle IDs** can differ by region. If `create-instances` fails, run:

```bash
aws lightsail get-blueprints --region "$AWS_REGION" --query 'blueprints[?contains(blueprintId, `ubuntu`)].blueprintId' --output text
aws lightsail get-bundles --region "$AWS_REGION" --query 'bundles[?contains(bundleId, `large`)].{id:bundleId,ram:ramSizeInGb,price:price}' --output table
```

Pick a **bundle** with **≥ 8 GB RAM** if you run the full compose stack comfortably.

---

## Step 3 — You: Lightsail firewall (console or CLI)

In **Lightsail → Networking** for `oziebot-lean-api`, allow:

- **TCP 22** — from **your IP only** (not 0.0.0.0/0 long term)
- **TCP 80, 443** — from **everywhere** (for `api.oziebot.com`)

CLI pattern (if supported in your CLI version):

```bash
aws lightsail open-instance-public-ports --instance-name oziebot-lean-api --region "$AWS_REGION" \
  --port-info fromPort=22,toPort=22,protocol=tcp,cidrs=YOUR.HOME.IP.ADDR/32

aws lightsail open-instance-public-ports --instance-name oziebot-lean-api --region "$AWS_REGION" \
  --port-info fromPort=80,toPort=80,protocol=tcp,cidrs=0.0.0.0/0

aws lightsail open-instance-public-ports --instance-name oziebot-lean-api --region "$AWS_REGION" \
  --port-info fromPort=443,toPort=443,protocol=tcp,cidrs=0.0.0.0/0
```

---

## Step 4 — Agent / you: install Docker + deploy Oziebot lean on the VPS

From your laptop (SSH uses the IP from Step 2):

```bash
export LEAN_IP="$(aws lightsail get-static-ip --static-ip-name oziebot-lean-api-ip --region us-east-1 --query staticIp.ipAddress --output text)"
ssh -i ~/.ssh/oziebot-lean.pem ubuntu@"$LEAN_IP" 'sudo apt-get update && sudo apt-get install -y docker.io docker-compose-plugin git && sudo usermod -aG docker ubuntu'
```

Copy the repo + **`.env.lean`** (never commit secrets), then on the server:

```bash
cd ~/oziebot
docker compose -f docker-compose.lean.yml -f docker-compose.lean.edge.yml --env-file .env.lean up -d --build
curl -sf https://api.oziebot.com/v1/ready   # only after DNS (Step 5)
```

Or use **`LEAN_USE_EDGE=1 ./infrastructure/lean/deploy-lean-host.sh`** after `LEAN_SSH` / `LEAN_REPO_PATH` are set.

**Database:** your lean Postgres is **empty** until you restore a dump from RDS (in-VPC `pg_dump`) or temporarily point **`DATABASE_URL`** at RDS **only during a controlled cutover** (update RDS SG to allow **only** the Lightsail static IP).

---

## Step 5 — You: GoDaddy DNS (cannot be automated here)

1. Remove **`api`** CNAME/ALIAS to **`*.elb.amazonaws.com`**.
2. **A** record **`api`** → **Lightsail static IP** from Step 2.
3. Wait for DNS, then verify: `curl -sf https://api.oziebot.com/v1/ready`.

Details: **`infrastructure/lean/GODADDY_API_LEAN.md`**.

---

## Step 6 — Agent: stop expensive compute (after HTTPS is green)

```bash
export AWS_PROFILE=oziebot AWS_REGION=us-east-1
./infrastructure/lean/aws-scale-ecs-to-zero.sh
```

**ALB still costs money** until you **delete** it in the console (manual confirmation). **RDS + ElastiCache** still bill until **stopped/deleted** with your **snapshot discipline**.

---

## Summary: “what access do you need?”

| Need | Who | Purpose |
|------|-----|---------|
| **IAM: Lightsail API** on `cryptobotty-deploy` (or dedicated “infra” user + profile) | **You (console)** | So CLI can **create** instance + static IP |
| **`AWS_PROFILE`** on the machine where Cursor runs | **You** | So commands execute **as that user** |
| **SSH private key** for the Lightsail instance | **You** | Log in and run Docker |
| **GoDaddy login** | **You** | Point **`api`** at the static IP |
| **Optional:** temporary **RDS SG** rule allowing **Lightsail IP** | **You** | Only if lean uses RDS during migration |

After **Step 1**, say **“Lightsail IAM is attached”** in chat and the agent can **run Step 2–4 and 6 commands** for you from this repo (you still do **GoDaddy** and any **RDS SG** / **ALB delete** confirmations).
