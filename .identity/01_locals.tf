locals {
  github = {
    org        = "pagopa"
    repository = "cruscotto-ingestor"
  }

  prefix         = "pagopa"
  domain         = "crusc8"
  location_short = "itn"
  product        = "${var.prefix}-${var.env_short}"

  app_name = "github-${local.github.org}-${local.github.repository}-${var.prefix}-${local.domain}-${var.env}-aks"

  aks_cluster = {
    name                = "${local.product}-${local.location_short}-${var.env}-aks"
    resource_group_name = "${local.product}-${local.location_short}-${var.env}-aks-rg"
  }

  postgres_db = {
    host           = "pagopa-${var.env_short}-itn-crusc8-flexible-postgresql.postgres.database.azure.com"
    port           = 5432
    name           = "cruscotto"
    schema         = "sert_ingestor"
    username       = "cruscotto"
    admin_username = "usrcrus8"
  }
}

