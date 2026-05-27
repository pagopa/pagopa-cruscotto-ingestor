variable "env" {
  type        = string
  description = "Environment: dev, uat, prod"
}

variable "env_short" {
  type        = string
  description = "Environment short name: d, u, p"
}

variable "prefix" {
  type        = string
  default     = "pagopa"
  description = "Project prefix (max 6 chars)"
  validation {
    condition = (
      length(var.prefix) <= 6
    )
    error_message = "Max length is 6 chars."
  }
}

variable "github_repository_environment" {
  type = object({
    protected_branches     = bool
    custom_branch_policies = bool
    reviewers_teams        = list(string)
  })
  description = "GitHub environment protection rules"
  default = {
    protected_branches     = false
    custom_branch_policies = true
    reviewers_teams        = ["pagopa-team-core"]
  }
}

