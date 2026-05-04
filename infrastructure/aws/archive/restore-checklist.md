# Restore checklist (future you)

Use together with `archive/snapshots/snapshot-*/` and `infrastructure/aws/**/*.yml|json|md`.

1. **Account & region** — Confirm `us-east-1` and account `608713827509` (see `service-map.yml`).
2. **Secrets** — Recreate Secrets Manager entries listed in `env-matrix.yml` / `github-actions-config.md` from your offline backup (or rotate every value and reconfigure Stripe/Coinbase/etc.).
3. **Network** — Subnets and security groups: use IDs from `github-actions-config.md` snapshot time, or recreate from `ec2/` JSON in your latest snapshot export.
4. **RDS** — Restore from manual snapshot; apply same subnet group / SG / parameter group as captured in `rds/` export.
5. **ElastiCache** — Recreate replication group; update `REDIS_URL` in GitHub/ECS to new primary endpoint.
6. **ECR** — Repositories listed in `service-map.yml`; push images from CI.
7. **ECS** — Cluster `oziebot-prod`; services per `service-map.yml`; task defs from Git + `ecs-task-definitions/` JSON in snapshot for last-known live revision.
8. **ALB / listeners / TG** — Cross-check `elbv2/` snapshot with `service-map.yml` (`oziebot-api-tg`, health `/v1/ready`).
9. **CloudFront + S3** — Frontend bucket and distribution ID in `github-actions-config.md`; reattach ACM cert if domain unchanged.
10. **DNS** — `godaddy-dns-template.yml` and CloudFront aliases.
11. **IAM** — `iam/*.json` in repo + `iam/` role/policy dumps in snapshot.
12. **GitHub Actions** — Re-enter `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` or switch to OIDC; restore all **variables** from `github-actions-config.md`.
