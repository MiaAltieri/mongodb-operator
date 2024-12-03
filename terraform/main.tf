resource "juju_application" "machine_mongodb" {
  name  = var.app_name
  model = var.juju_model_name

  charm {
    name     = "mongodb"
    channel  = var.channel
    revision = var.revision
    base     = var.base
  }

  storage_directives = {
    mongodb = var.storage_size
  }

  units       = var.units
  constraints = var.constraints
  config      = var.config
}
