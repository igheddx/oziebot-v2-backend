# GitHub Actions AWS config

This file maps each GitHub Actions secret/variable to either:

1. the exact current production value, or
2. where to find/create it in AWS.

Set these in **GitHub -> repo -> Settings -> Secrets and variables -> Actions**.

## Frontend repo: `igheddx/oziebot-v2-frontend`

### GitHub secrets

| Name | Where it comes from |
| --- | --- |
| `AWS_ACCESS_KEY_ID` | IAM -> Users -> `cryptobotty-deploy` -> Security credentials -> Access keys. Use the access key ID from the deploy user you want GitHub Actions to use. |
| `AWS_SECRET_ACCESS_KEY` | Same access key pair as above. AWS only shows this once when the key is created, so if you do not already have it saved, create a new access key for the deploy user and store the new secret in GitHub. |

### GitHub variables

| Name | Value | Where to find it |
| --- | --- | --- |
| `AWS_REGION` | `us-east-1` | AWS region used by the live stack. |
| `FRONTEND_S3_BUCKET` | `app.oziebot.com` | S3 -> Buckets -> the static site bucket serving the app. |
| `FRONTEND_CLOUDFRONT_DISTRIBUTION_ID` | `E3JE0URVE1J1DJ` | CloudFront -> Distributions -> alias `app.oziebot.com`. |
| `NEXT_PUBLIC_API_URL` | `https://api.oziebot.com` | Public API base for the production frontend. |

## Backend repo: `igheddx/oziebot-v2-backend`

### GitHub secrets

| Name | Where it comes from |
| --- | --- |
| `AWS_ACCESS_KEY_ID` | IAM -> Users -> `cryptobotty-deploy` -> Security credentials -> Access keys. |
| `AWS_SECRET_ACCESS_KEY` | Same access key pair as above. Create a new one if you do not have the secret saved. |

### GitHub variables

| Name | Value | Where to find it |
| --- | --- | --- |
| `AWS_REGION` | `us-east-1` | Live region. |
| `ECS_CLUSTER` | `oziebot-prod` | ECS -> Clusters. |
| `ECS_EXECUTION_ROLE_ARN` | `arn:aws:iam::608713827509:role/oziebot-ecs-execution-role` | ECS -> Task definitions -> current API task definition -> task execution role. |
| `ECS_TASK_ROLE_ARN` | `arn:aws:iam::608713827509:role/oziebot-ecs-task-role` | ECS -> Task definitions -> current API task definition -> task role. |
| `DATABASE_URL_SECRET_ARN` | `arn:aws:secretsmanager:us-east-1:608713827509:secret:oziebot/prod/shared/database-url-PSa309` | Secrets Manager -> `oziebot/prod/shared/database-url`. |
| `JWT_SECRET_SECRET_ARN` | `arn:aws:secretsmanager:us-east-1:608713827509:secret:oziebot/prod/api/jwt-secret-Gu1zVw` | Secrets Manager -> `oziebot/prod/api/jwt-secret`. |
| `STRIPE_SECRET_KEY_SECRET_ARN` | `arn:aws:secretsmanager:us-east-1:608713827509:secret:oziebot/prod/api/stripe-secret-key-mGPOQj` | Secrets Manager -> `oziebot/prod/api/stripe-secret-key`. |
| `STRIPE_WEBHOOK_SECRET_SECRET_ARN` | `arn:aws:secretsmanager:us-east-1:608713827509:secret:oziebot/prod/api/stripe-webhook-secret-qm3WKH` | Secrets Manager -> `oziebot/prod/api/stripe-webhook-secret`. |
| `EXCHANGE_CREDENTIALS_ENCRYPTION_KEY_SECRET_ARN` | `arn:aws:secretsmanager:us-east-1:608713827509:secret:oziebot/prod/shared/exchange-credentials-encryption-key-EYpvlY` | Secrets Manager -> `oziebot/prod/shared/exchange-credentials-encryption-key`. |
| `SMS_WEBHOOK_URL_SECRET_ARN` | `arn:aws:secretsmanager:us-east-1:608713827509:secret:oziebot/prod/alerts/sms-webhook-url-OP2zsg` | Secrets Manager -> `oziebot/prod/alerts/sms-webhook-url`. |
| `SLACK_WEBHOOK_URL_SECRET_ARN` | `arn:aws:secretsmanager:us-east-1:608713827509:secret:oziebot/prod/alerts/slack-webhook-url-OzZy4p` | Secrets Manager -> `oziebot/prod/alerts/slack-webhook-url`. |
| `TELEGRAM_BOT_TOKEN_SECRET_ARN` | `arn:aws:secretsmanager:us-east-1:608713827509:secret:oziebot/prod/alerts/telegram-bot-token-Pvxh0J` | Secrets Manager -> `oziebot/prod/alerts/telegram-bot-token`. |
| `ECS_SUBNETS` | `subnet-029221f44a3665e70,subnet-0670572a23c04d833` | ECS -> Service -> Networking for any current prod service in `oziebot-prod`. Use comma-separated subnet IDs with no spaces. |
| `ECS_SECURITY_GROUPS` | `sg-0c13b89a1c6c657b9` | ECS -> Service -> Networking for any current prod service in `oziebot-prod`. Use comma-separated security group IDs with no spaces. |
| `ECS_ASSIGN_PUBLIC_IP` | `ENABLED` | ECS -> Service -> Networking -> Public IP. Current live services use `ENABLED`. |
| `REDIS_URL` | `redis://master.oziebot-prod-redis.je1lax.use1.cache.amazonaws.com:6379/0` | ElastiCache -> Replication groups -> `oziebot-prod-redis` -> primary endpoint. |
| `OBSERVABILITY_S3_BUCKET` | `oziebot-prod-observability` | S3 -> Buckets -> create a dedicated private bucket for runtime/trade-log observability snapshots and history. |
| `OBSERVABILITY_S3_PREFIX` | `observability` | S3 key prefix inside the bucket. Keep this stable so latest/history keys stay under one namespace. |
| `OBSERVABILITY_S3_REGION` | `us-east-1` | Region for the observability bucket. Match the bucket region. |

## Notes

- The backend workflow stores **ARNs** in GitHub variables, not raw secret values. The task definitions pull the actual secret contents from AWS Secrets Manager at deploy/runtime.
- The only raw secrets currently expected by the workflows themselves are `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
- If you prefer a cleaner setup later, switch both repos from long-lived AWS keys to GitHub OIDC + an assumable IAM role.
- When `OBSERVABILITY_S3_BUCKET` is set, runtime heartbeats, trade-log events, signal samples/summaries, and their recent history move to S3 while Redis remains the hot cache/coordination layer.
- Add S3 task-role access for the backend ECS tasks on that bucket/prefix: `s3:PutObject`, `s3:GetObject`, and `s3:ListBucket` scoped to the observability prefix.
