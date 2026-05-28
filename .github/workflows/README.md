# Workflow Notes – Cruscotto Ingestor

## Main Workflow: `04_release_and_deploy.yml`

**This is the only workflow you need to use.**

### How to Use

1. Go to **GitHub Actions** → **release-and-deploy**
2. Click **"Run workflow"**
3. Select environment: `dev` / `uat` / `prod`
4. (Optional) Enter custom Docker image tag
5. Click **"Run"**

### What It Does (Automated)

```
1. Checks out code from branch
2. Reads Azure Key Vault secrets (db-cruscotto-password, adx-app-id, adx-app-key)
3. Runs: mvn -B verify (with secrets injected)
4. Creates Docker image
5. Pushes to GHCR (container registry)
6. Helmint validation (helm values)
7. Deploys to Kubernetes (via Helm)
8. Verifies deployment rollout
```

### Deploy Guardrails

- **Prod deployment** allowed only from `main` branch (enforced)
- **Other environs** (dev/uat) can deploy from any branch
- Helm manifest validation runs before deployment
- Maven tests required before image build

---

## GitHub Configuration Required

### Secrets (Repository or Organization level)

```
AZURE_CLIENT_ID          # Service Principal client ID (for OIDC)
AZURE_TENANT_ID          # Azure tenant ID
AZURE_SUBSCRIPTION_ID    # Azure subscription ID
KUBE_CONFIG              # Base64-encoded kubeconfig file
```

### Environment Variables (per environment: dev, uat, prod)

```
KEYVAULT_NAME    # Name of Azure Key Vault (e.g., pagopa-d-itn-crusc8-kv)
K8S_NAMESPACE    # Kubernetes namespace (e.g., crusc8)
```

### Azure Key Vault Secrets (Auto-created by setup script)

Each vault must contain:
```
db-cruscotto-password    # PostgreSQL password
adx-app-id               # Azure Data Explorer Service Principal ID
adx-app-key              # Azure Data Explorer Service Principal Secret
```

**Setup these using**:
```powershell
.\scripts\setup-azure-keyvault.ps1 -Environment all
```

---

## GitHub Environments Setup

Create three GitHub Environments: `dev`, `uat`, `prod`

### Pro Tip: Production Approval

Settings → Environments → `prod`:
- Enable **Required reviewers** (at least 1)
- Optional: Restrict to `main` branch only (already enforced in workflow)

This gives **manual approval gate** before prod deployments.

---

## Security Notes

✅ **No secrets stored in code**
- All sensitive values read from Azure Key Vault at runtime
- GitHub Actions uses OIDC federated credentials (no PAT needed)

✅ **Minimal permissions**
- Service Principal reads only secrets necessary
- Kubernetes Service Account uses Workload Identity

✅ **Audit trail**
- All deployments traceable via commit + workflow run
- Azure Key Vault logs all secret accesses

---

## Troubleshooting

**"Failed to authenticate with Azure AD"**
→ Verify AZURE_CLIENT_ID, AZURE_TENANT_ID in GitHub secrets

**"Key Vault not found"**
→ Check KEYVAULT_NAME variable matches actual vault name

**"KUBE_CONFIG decoding failed"**
→ Ensure KUBE_CONFIG secret is valid base64 and valid kubeconfig

**"Helm deployment fails"**
→ Check `helm/values-{dev|uat|prod}.yaml` syntax with: `helm lint helm -f helm/values-dev.yaml`

