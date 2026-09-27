# Git Branching Strategy — Data Engineering Teams

A trunk-based-with-integration-branch model, adapted for the fact that a data pipeline
change carries risk a normal app change doesn't: it can silently corrupt or duplicate
data long after the deploy, not just crash at request time.

## Branch layout

```
main                    <- always deployable; protected; maps to the PROD Databricks workspace
  │
  └── develop           <- integration branch; maps to the STAGING Databricks workspace
        │
        ├── feature/<desc>   <- one pipeline change (a new source, a new transform, a DQ check)
        ├── bugfix/<desc>    <- fixes a defect in an existing pipeline
        └── hotfix/<desc>    <- branched from main directly, for a prod incident fix

  release/2026.02       <- cut from develop to harden before promoting to main; tagged on merge
```

| Branch | Maps to (Databricks target) | Who can merge into it |
|---|---|---|
| `feature/*`, `bugfix/*` | `dev` (personal/shared dev workspace) | anyone, freely |
| `develop` | `staging` | via PR, 1 approval, CI green |
| `release/*` | `staging` (frozen for hardening) | via PR from develop only |
| `main` | `prod` | via PR from `release/*` or `hotfix/*`, 2 approvals, CI green |

This mirrors the pattern in `databricks/databricks.yml` — each bundle *target*
(`dev`/`staging`/`prod`) corresponds exactly to one branch tier, so "which branch am I
on" always answers "which environment does this deploy to."

## Naming conventions

- `feature/<short-desc>` — e.g. `feature/add-price-outlier-check`
- `bugfix/<short-desc>` — e.g. `bugfix/fix-null-category-crash`
- `hotfix/<short-desc>` — e.g. `hotfix/rollback-bad-merge-key`
- Commit messages: [Conventional Commits](https://www.conventionalcommits.org/) —
  `feat: ...`, `fix: ...`, `test: ...`, `docs: ...`, `refactor: ...` — makes it trivial
  to generate a changelog and to spot risky commit types (`fix`, `refactor`) in a
  release diff.
- Tag every merge to `main` with a semantic version: `v1.4.0`, `v1.4.1` — so any
  production pipeline run can be traced back to the exact code that produced it
  (put the tag/commit SHA in `pipeline_runs.message` or equivalent run metadata).

## One logical change per PR

A PR should touch **one pipeline stage, one transformation function, or one DQ check** —
not "refactor everything + add a feature." This keeps `git bisect` useful when a bad
batch shows up three weeks later and someone has to find which commit caused it.

## PR checklist (data-specific — beyond the usual code review)

Every PR template should force the author to answer:

- [ ] **What tables/topics does this touch?** (so reviewers know the blast radius)
- [ ] **Is this change backward compatible with existing data?** (a renamed/retyped
      column breaks every downstream consumer silently until they hit a schema error)
- [ ] **What's the backfill plan?** if this changes historical logic, does it need to
      be re-run over past dates? (see `run_batch()` / `pipeline_runs` in `src/pipeline`)
- [ ] **What data-quality checks were added or changed?** (link the test in `tests/`)
- [ ] **Unit tests included and passing locally** (`./scripts/run_local_ci.sh`)

## Never commit

- Secrets, tokens, connection strings — use Databricks secret scopes
  (`dbutils.secrets.get(...)`) or GitHub Actions encrypted secrets, referenced by name
  only in code.
- Generated data, `.db`/`.sqlite` files, or notebook output cells — see `.gitignore`.
  A notebook's *code* is reviewable; its last-run output is noise that bloats diffs.

## Example workflow

```bash
git checkout -b feature/add-price-outlier-check develop
# ... edit src/pipeline/quality.py, add a test in tests/test_quality.py ...
./scripts/run_local_ci.sh                 # green before you even open the PR
git add src/pipeline/quality.py tests/test_quality.py
git commit -m "feat: quarantine extreme price outliers in validate()"
git push -u origin feature/add-price-outlier-check
# open a PR: develop <- feature/add-price-outlier-check
```
