resource "null_resource" "preamble" {
  provisioner "local-exec" {
    command = <<-EOT
    sudo snap install juju-wait --classic || true
    EOT
  }
}

resource "juju_application" "self-signed-certificates" {
  charm {
    name    = "self-signed-certificates"
    channel = "latest/stable"
  }
  model      = var.model_name
  depends_on = [null_resource.preamble]
}

resource "juju_application" "data-integrator" {
  charm {
    name    = "data-integrator"
    channel = "latest/stable"
  }
  model      = var.model_name
  depends_on = [null_resource.preamble]
}

resource "juju_application" "grafana-agent" {
  charm {
    name    = "grafana-agent"
    channel = "latest/stable"
  }
  model      = var.model_name
  depends_on = [null_resource.preamble]
}

resource "juju_application" "s3-integrator" {
  charm {
    name    = "s3-integrator"
    channel = "latest/stable"
  }
  model      = var.model_name
  depends_on = [null_resource.preamble]
}

resource "juju_application" "mongos" {
  charm {
    name    = "mongos"
    channel = "6/stable"
  }
  model      = var.model_name
  depends_on = [null_resource.preamble]
}