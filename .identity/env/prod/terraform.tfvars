env       = "prod"
env_short = "p"
prefix    = "pagopa"

github_repository_environment = {
  protected_branches     = true
  custom_branch_policies = false
  reviewers_teams        = ["pagopa-team-core"]  # Manual approval required for prod
}

