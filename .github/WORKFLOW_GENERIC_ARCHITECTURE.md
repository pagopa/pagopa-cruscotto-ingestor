# Workflow Architecture – Generic vs Specific

## TL;DR

**Use `04_release_and_deploy.yml` for ALL environments.**

No need for `05_build_dev.yml` (removed).

---

## Why Generic Workflow?

### ❌ BEFORE (Problematico)
```
05_build_dev.yml    ← Solo dev
04_release_and_deploy.yml   ← Solo deploy (manual)
```

**Problema**: Duplicazione, difficile mantenere, confusione su quale usare.

---

### ✅ AFTER (Corretto)
```
04_release_and_deploy.yml   ← Build + Test + Deploy (TUTTI gli ambienti)
```

**Vantaggio**: Un workflow, tutti gli ambienti, parametrizzato.

---

## How Generic Workflow Works

### Input Parameter: Environment

Quando avvii il workflow, scegli l'ambiente:

```yaml
on:
  workflow_dispatch:
    inputs:
      environment:
        type: choice
        options: [dev, uat, prod]  ← Tu scegli
```

### Dynamic Variable Resolution

```yaml
jobs:
  build-and-push:
    environment: ${{ github.event.inputs.environment }}  # ← dev/uat/prod
    
    steps:
      # Usa le variabili di QUELL'ENVIRONMENT
      - name: Get secrets from Key Vault
        with:
          keyvault: ${{ vars.KEYVAULT_NAME }}  # ← pagopa-d-itn-crusc8-kv (per dev)
```

Se scegli `prod`, usa `pagopa-p-itn-crusc8-kv`.
Se scegli `dev`, usa `pagopa-d-itn-crusc8-kv`.
Tutto automaticamente!

---

## Full Flow: Dev → UAT → Prod (Stesso Workflow)

```
Step 1: Push code → develop
    ↓
Step 2: Manual trigger:
    Go to GitHub Actions → 04_release_and_deploy
    Run workflow → ambiente: dev
    ↓
Step 3: Workflow esegue:
    - Checkout code
    - Azure login
    - Get secrets from pagopa-d-itn-crusc8-kv (dev vault)  ← Dinamico!
    - mvn verify
    - Docker build & push
    - Helm deploy to dev K8s
    ↓
Step 4: Test on dev, then:
    Go to GitHub Actions → 04_release_and_deploy
    Run workflow → ambiente: uat
    ↓
Step 5: Workflow esegue:
    - (stessi step, ma con pagopa-u-itn-crusc8-kv)  ← Diverso vault!
    - Deploy to uat K8s
    ↓
Step 6: Final release:
    Merge to main branch
    Go to GitHub Actions → 04_release_and_deploy
    Run workflow → ambiente: prod
    ↓
Step 7: Workflow esegue:
    - Verifica che branch = main (altrimenti fallisce)
    - (stessi step, ma con pagopa-p-itn-crusc8-kv)
    - Deploy to prod K8s
```

**Same workflow file, different environments, different vaults, different K8s clusters.**

---

## Configuration Per Environment

### GitHub Environments (Settings → Environments)

```
dev
├─ Variables:
│  ├─ KEYVAULT_NAME = pagopa-d-itn-crusc8-kv
│  └─ K8S_NAMESPACE = crusc8
├─ Protection rules: none (optional)
└─ Approvers: none

uat
├─ Variables:
│  ├─ KEYVAULT_NAME = pagopa-u-itn-crusc8-kv
│  └─ K8S_NAMESPACE = crusc8
├─ Protection rules: optional
└─ Approvers: optional

prod
├─ Variables:
│  ├─ KEYVAULT_NAME = pagopa-p-itn-crusc8-kv
│  └─ K8S_NAMESPACE = crusc8
├─ Protection rules: REQUIRED
│  ├─ Required reviewers ≥ 1
│  └─ Deployment branches: main only
└─ Approvers: YES (manual approval gate)
```

---

## Helm Values Auto-Selection

When you run the workflow for `dev`, Helm automatically uses `helm/values-dev.yaml`:

```bash
helm upgrade --install cruscotto-ingestor helm \
  --namespace crusc8 \
  -f helm/values-${{ github.event.inputs.environment }}.yaml  # ← dev/uat/prod
```

Same mechanism for all three enviroments!

---

## Secret Injection Flow (Same for All Envs)

```
User selects: dev
    ↓
Workflow reads env variable:
    KEYVAULT_NAME = pagopa-d-itn-crusc8-kv
    ↓
Workflow calls Azure Key Vault:
    Give me: db-cruscotto-password, adx-app-id, adx-app-key
    ↓
Key Vault responds:
    Password: qualification_dashboard
    AppId: 8677a3ca-...
    AppKey: qKa8Q~...
    ↓
Workflow passes to Maven:
    export SPRING_DATASOURCE_PASSWORD=qualification_dashboard
    ↓
Maven compiles with secrets
    ↓
Docker image created (NO secrets inside!)
    ↓
Helm reads values-dev.yaml:
    keyvault.name: pagopa-d-itn-crusc8-kv
    envSecret.PASSWORD: db-cruscotto-password  ← name, not value
    ↓
Kubernetes Workload Identity:
    Read secret db-cruscotto-password from pagopa-d-itn-crusc8-kv
    ↓
Pod receives:
    SPRING_DATASOURCE_PASSWORD=qualification_dashboard
```

---

## Using the Workflow

### Local Test (Dev)
```
1. GitHub Actions → release-and-deploy → Run workflow
2. Choose: dev
3. Leave image_tag empty (uses commit SHA)
4. Click Run
5. Watch the build & deploy
6. Verify on dev K8s: kubectl get pods -n crusc8
```

### Stage Test (UAT)
```
1. GitHub Actions → release-and-deploy → Run workflow
2. Choose: uat
3. (Optional) Specify image_tag if deployed dev was good
4. Click Run
```

### Production Deploy (Prod)
```
1. Ensure code is on main branch
2. GitHub Actions → release-and-deploy → Run workflow
3. Choose: prod
4. (Optional) Specify image_tag
5. Click Run
6. ← Requires manual approval (if protection rule enabled)
7. Approve in GitHub
8. Watch deployment to prod
```

---

## Benefits of Generic Workflow

| Aspect | Specific (❌ Old) | Generic (✅ New) |
|--------|------------------|-----------------|
| # of Workflows | 1 per env (3 total) | 1 for all |
| Maintenance | Repeat fixes in 3 places | Fix once, applies to all |
| Parameter changes | Update 3 files | Update 1 file |
| User experience | "Which button do I click?" | Clear: one button, pick env |
| Env-specific logic | Hardcoded | Parameterized (clean) |
| Test before prod | Must separately build | Same build, different K8s |

---

## Removed Files

- `05_build_dev.yml` ← Deleted (not needed, use generic workflow)

All environment-specific logic moved to **GitHub Environment variables** instead of hardcoded in workflow.

---

## Next Steps

1. ✅ Setup GitHub Environments (dev, uat, prod) with variables
2. ✅ Configure GitHub Secrets (AZURE_*, KUBE_CONFIG)
3. ✅ Setup Azure Key Vaults and secrets (./scripts/setup-azure-keyvault.ps1)
4. Test:
   ```
   GitHub Actions → release-and-deploy → Run workflow → dev
   ```

Done!

