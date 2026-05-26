# Workflow notes

## Deploy guardrails implemented

- `prod` deployment is allowed only when workflow runs from `main`.
- Deploy workflows run Helm lint before deployment.
- `04_release_and_deploy.yml` runs `mvn verify` before image build/push.

## GitHub configuration required

Configure repository or environment secrets/variables used by workflows:

- Secrets:
  - `KUBE_CONFIG` (base64 kubeconfig)
  - `LIQUIBASE_URL`
  - `LIQUIBASE_USERNAME`
  - `LIQUIBASE_PASSWORD`
- Variables:
  - `K8S_NAMESPACE`

## Recommended environment protection rules

Set up GitHub Environments (`dev`, `uat`, `prod`) and enable:

- `prod`: required reviewers (at least 1), optional wait timer.
- `uat`: optional required reviewers.
- deployment branches restriction as needed.

