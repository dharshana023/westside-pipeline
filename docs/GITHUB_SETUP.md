# Connecting this repo to GitHub

## 1. Create the remote repo (no files, so it doesn't conflict with what you already have)

On github.com: **New repository** → name it (e.g. `westside-pipeline`) → do **not**
initialize with a README/.gitignore/license → Create.

## 2. Point your local repo at it and push both branches

```bash
cd westside-pipeline   # the folder from the zip you already unzipped
git remote add origin git@github.com:<your-username>/westside-pipeline.git
git push -u origin main
git push -u origin develop
```

(Using HTTPS instead of SSH? `git remote add origin https://github.com/<you>/westside-pipeline.git`)

## 3. Protect `main` and `develop`

Repo → **Settings → Branches → Add branch ruleset** (or "Add rule" on older GitHub UI):
- Require a pull request before merging
- Require status checks to pass (once step 5 below has run once, `test` will show up
  as a selectable required check)
- For `main` specifically: require 2 approvals (matches `docs/GIT_BRANCHING_STRATEGY.md`)

## 4. Add the secrets the deploy job needs (only if you want the prod-deploy job to run)

Repo → **Settings → Secrets and variables → Actions → New repository secret**:
- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`

Without these, the `test` job in `.github/workflows/ci.yml` still runs fine on every
push/PR — only the `deploy-prod` job (which only fires on pushes to `main`) needs them.
If you don't want the deploy job at all yet, just ignore/delete that job block in the
workflow file.

## 5. Trigger the first run

```bash
git checkout -b feature/ci-smoke-test develop
git commit --allow-empty -m "chore: trigger first CI run"
git push -u origin feature/ci-smoke-test
```

Open a PR (`develop` ← `feature/ci-smoke-test`) on GitHub. Check the **Actions** tab —
you should see `Data Pipeline CI` running lint + pytest. Once it's green, merge.

## 6. Day-to-day flow from here

Every future change follows `docs/GIT_BRANCHING_STRATEGY.md`:

```bash
git checkout -b feature/<desc> develop
# ... edit src/pipeline/*.py and the matching tests/test_*.py ...
./scripts/run_local_ci.sh          # green locally first
git add -A && git commit -m "feat: ..."
git push -u origin feature/<desc>
# open PR into develop -> GitHub Actions runs the same checks automatically
```
