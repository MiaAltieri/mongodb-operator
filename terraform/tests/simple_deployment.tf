module "mongodb" {
  source   = "../"
  app_name = var.app_name
  juju_model_name    = var.juju_model_name
  units    = var.simple_mongodb_units
  config = {
    profile = "testing"
  }

  channel = "6/edge"

}

resource "null_resource" "simple_deployment_juju_wait_deployment" {
  provisioner "local-exec" {
    command = <<-EOT
    EOT
  }
}
