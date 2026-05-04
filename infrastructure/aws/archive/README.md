# AWS archive (come-back-later pack)

This folder is the **home for offline backups** of your Oziebot AWS footprint. Two layers:

## 1. Already in Git (source of truth for “how we meant to deploy”)

Version-controlled under `infrastructure/aws/`:

| Path | Purpose |
| --- | --- |
| `backend/service-map.yml` | Account `608713827509`, cluster `oziebot-prod`, ECS service names, ECR repos, ALB/TG naming |
| `backend/env-matrix.yml` | Env var layout; **production** points at Secrets Manager names (not values) |
| `backend/task-definitions/*.json` | ECS task definition **templates** used by CI (image tags updated on deploy) |
| `iam/*.json` | IAM policy fragments for deploy user / roles |
| `github-actions-config.md` | GitHub Actions variables, secret ARNs, subnets, SGs, Redis endpoint, CloudFront ID, S3 buckets |
| `godaddy-dns-template.yml` | DNS record template |

Commit and push these repos before you tear anything down so the templates survive even if local disks change.

## 2. Live snapshot (run once before shutdown)

Run the export script **from your machine** with credentials that can read the account (deploy or admin read-only):

```bash
cd infrastructure/aws/scripts
chmod +x export-aws-snapshot.sh
AWS_PROFILE=oziebot ./export-aws-snapshot.sh
```

Output: `archive/snapshots/snapshot-YYYYmmdd-HHMMSS/` (gitignored).

By default the script **does not** call `GetSecretValue`; it only lists secret **names and ARNs**. To also download secret strings (risky—treat files like passwords), run:

```bash
EXPORT_SECRET_VALUES=1 AWS_PROFILE=oziebot ./export-aws-snapshot.sh
```

Store that directory **outside** public repos—e.g. encrypted USB, password manager attachment, or a private encrypted cloud folder.

### Before you delete RDS

- Take a **manual snapshot** in the RDS console (or `create-db-snapshot`), or ensure automated snapshots retention meets your needs.
- Note the **DB instance identifier**, engine version, parameter group, and subnet group from the snapshot JSON the script writes.

### After shutdown

Billing still accrues for: snapshots (RDS/EBS), unused EIPs, NAT Gateways left on, empty ELBs, **Secrets Manager** secrets, **S3** objects, **ECR** images. Delete or consolidate those when you are sure you do not need them.

### Restore later (high level)

1. Recreate VPC/subnets/SGs or reuse the same if you only stopped services.
2. Restore RDS from snapshot (or redeploy empty DB + migrate).
3. Recreate ElastiCache or point `REDIS_URL` at new cluster.
4. Push ECR images from CI; register new task definition revisions from `task-definitions/*.json` + console diffs from `describe-task-definition` in your snapshot.
5. Recreate ECS services using `service-map.yml` names.
6. Re-seed Secrets Manager from your **offline** secret export if you saved one (or rotate all secrets and re-enter integrations).
7. Re-point DNS (`godaddy-dns-template.yml` + CloudFront + ACM as before).

See `restore-checklist.md` for a terse ordered list.
