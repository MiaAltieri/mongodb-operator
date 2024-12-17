# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

output "app_name" {
  description = "Name of the deployed application."
  value       = juju_application.mongodb.name
}

# Provided integration endpoints

output "database_endpoint" {
  description = "Name of the endpoint to provide the mongodb_client interface."
  value       = "database"
}

output "cos_agent_endpoint" {
  description = "Name of the endpoint to provide the cos_agent interface."
  value       = "cos-agent"
}

output "config_server_endpoint" {
  description = "Name of the endpoint to provide the shards interface."
  value       = "config-server"
}

output "cluster_endpoint" {
  description = "Name of the endpoint to provide the config-server interface."
  value       = "cluster"
}

# Required integration endpoints

output "certificates_endpoint" {
  description = "Name of the endpoint to provide the tls-certificates interface."
  value       = "certificates"
}

output "s3_credentials_endpoint" {
  description = "Name of the endpoint to provide the s3 interface."
  value       = "s3-credentials"
}

output "sharding_endpoint" {
  description = "Name of the endpoint to provide the shards interface."
  value       = "sharding"
}