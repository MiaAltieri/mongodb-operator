variable "model_name" {
  description = "Model name"
  type        = string
}

variable "config_server_app_name" {
  description = "config-server app name"
  type        = string
  default     = "config-server"
}

variable "config_server_replicas" {
  description = "Node count"
  type        = number
  default     = 1
}

variable "shards" {
  description = "A list of shards containing their name and number of replicas"
  type = list(object({
    name     = string
    replicas = number
  }))
  default = [
    { name = "shard0", replicas = 2 },
    { name = "shard1", replicas = 1 }
  ]
}



