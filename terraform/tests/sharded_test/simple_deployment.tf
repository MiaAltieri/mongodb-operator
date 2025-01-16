locals {
  mongodb_apps = {
    "config-server" = {
      app_name = var.config_server_app_name
      units    = var.config_server_units
      role = "config-server"
    }
    "shard-one" = {
      app_name = var.shard_one_app_name
      units    = var.shard_one_units
      role = "shard"
    }
    "shard-two" = {
      app_name = var.shard_two_app_name
      units    = var.shard_two_units
      role = "shard"
    }
  }
}

module "mongodb" {
  for_each = local.mongodb_apps
  source   = "../../"
  app_name = each.value.app_name
  model    = var.model_name
  units    = each.value.units
  config = {
    role = each.key 
  }
  channel  = "6/edge"
}


resource "juju_integration" "data-integrator_mongos-integration" {
  model = var.model_name

  application {
    name = juju_application.data-integrator.name
  }
  application {
    name = juju_application.mongos.name
  }
  depends_on = [
    juju_application.data-integrator,
    juju_application.mongos
  ]

}

resource "juju_integration" "config-server_integrations" {
  for_each = tomap({
    "shard-one" = {
      app_name = var.shard_one_app_name
    }
    "shard-two" = {
      app_name = var.shard_two_app_name
    }
  })

  model = var.model_name

  application {
    name = var.config_server_app_name
    endpoint = "config-server"
  }

  application {
    name = each.value.app_name
    endpoint = "sharding"
  }

  depends_on = [
    module.mongodb,
  ]
}

resource "juju_integration" "mongodb_mongos-integration" {
  model = var.model_name

  application {
    name = juju_application.mongos.name
  }
  application {
    name = var.config_server_app_name
  }
  depends_on = [
    juju_application.mongos,
    module.mongodb,
    juju_integration.data-integrator_mongos-integration
  ]

}

resource "juju_integration" "tls-operator_mongodb-integration" {
  for_each = merge(
    local.mongodb_apps,
    {
      "mongos" = {
        app_name = "mongos"
        units    = 1
      }
    }
  )

  model = var.model_name

  application {
    name = juju_application.self-signed-certificates.name
  }

  application {
    name = each.value.app_name
  }

  depends_on = [
    juju_application.self-signed-certificates,
    juju_integration.mongodb_mongos-integration,
    juju_integration.config-server_integrations
  ]
}

resource "juju_integration" "s3-integrator_mongodb-integration" {
  model = var.model_name

  application {
    name = juju_application.s3-integrator.name
  }
  application {
    name = var.config_server_app_name
  }
  depends_on = [
    juju_application.s3-integrator,
    juju_integration.config-server_integrations,
  ]

}

resource "juju_integration" "grafana_agent_mongodb_integration" {
  for_each = local.mongodb_apps

  model = var.model_name

  application {
    name = juju_application.grafana-agent.name
  }

  application {
    name = each.value.app_name
  }

  depends_on = [
    juju_application.grafana-agent,
    module.mongodb
  ]
}

resource "null_resource" "juju_wait_deployment" {
  provisioner "local-exec" {
    command = <<-EOT
    juju-wait -v --model ${var.model_name}
    EOT
  }

  depends_on = [juju_integration.tls-operator_mongodb-integration]
}
