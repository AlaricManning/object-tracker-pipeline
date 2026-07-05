# AWS identity setup for the pipeline

The pipeline runs as its own IAM user with the minimum permissions the sync
needs — nothing more. This page is the one-time setup walkthrough.

## The security model

Three identities, three blast radii:

| Profile | Used by | Can | Cannot |
|---|---|---|---|
| `object-tracker-edge` | edge devices | put/get objects under `raw/*` | list anything, delete anything, touch `catalog/` |
| `object-tracker-pipeline` | this repo's sync | list+read `raw/*`, list+read+write `catalog/*` | write to `raw/`, delete anything, touch other prefixes/buckets |
| `default` (admin) | you, interactively | everything | — |

Leaked pipeline credentials cannot destroy captured data: there is no
`s3:DeleteObject` anywhere in its policy (the catalog design is append-only —
re-processing overwrites by writing the same keys, which needs only
`PutObject`). Never put credentials in tracked files; never use the root user.

## One-time setup (admin console)

1. **Create the policy.** IAM → Policies → Create policy → JSON tab → paste
   the contents of [`infra/iam/pipeline-policy.json`](../infra/iam/pipeline-policy.json)
   → name it `object-tracker-pipeline-policy` → Create.

2. **Create the user.** IAM → Users → Create user → name
   `object-tracker-pipeline` → **no console access** → Next →
   "Attach policies directly" → select `object-tracker-pipeline-policy` →
   Create user.

3. **Create an access key.** Open the user → Security credentials →
   Create access key → use case "Command Line Interface (CLI)" → Create.
   Copy both values now; the secret is shown once.

4. **Add the profile locally.** Append to `~/.aws/credentials`
   (never to any file in a repo):

   ```ini
   [object-tracker-pipeline]
   aws_access_key_id = <access key id>
   aws_secret_access_key = <secret access key>
   ```

   And if you don't have a default region configured, add to `~/.aws/config`:

   ```ini
   [profile object-tracker-pipeline]
   region = us-east-1
   ```

## Verify the fence posts

Each command checks one edge of the permission boundary:

```bash
# Allowed: list and read the raw area
AWS_PROFILE=object-tracker-pipeline aws s3 ls s3://object-tracker-am/raw/

# Denied: list outside the allowed prefixes (bucket root)
AWS_PROFILE=object-tracker-pipeline aws s3 ls s3://object-tracker-am/

# Denied: write into the raw area (that's the edge's job)
echo x | AWS_PROFILE=object-tracker-pipeline \
    aws s3 cp - s3://object-tracker-am/raw/should-fail.txt

# Denied: delete anything at all
AWS_PROFILE=object-tracker-pipeline \
    aws s3 rm s3://object-tracker-am/raw/anything.txt
```

The two "Denied" checks should fail with `AccessDenied` — if they succeed,
stop and re-check the policy attachment.

## First real run

```bash
# See what it would do
AWS_PROFILE=object-tracker-pipeline python -m object_tracker_pipeline.sync \
    --bucket object-tracker-am --dry-run

# Do it
AWS_PROFILE=object-tracker-pipeline python -m object_tracker_pipeline.sync \
    --bucket object-tracker-am

# Admire the catalog (admin profile, since the pipeline can't list root)
aws s3 ls s3://object-tracker-am/catalog/ --recursive
```

## Later additions

- **(Athena/Glue)** will extend `object-tracker-pipeline-policy` with
  scoped Glue-catalog and Athena statements — edit the policy JSON in place,
  no new user or keys needed.
- If sync ever runs from GitHub Actions, use OIDC role assumption (no stored
  secrets) instead of copying these keys anywhere.