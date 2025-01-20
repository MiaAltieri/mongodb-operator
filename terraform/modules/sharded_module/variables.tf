variable "model_name" {
  description = "Model name"
  type        = string
}

variable "config_server_app_name" {
  description = "mongodb app name"
  type        = string
  default     = "config-server"
}

variable "config_server_units" {
  description = "Node count"
  type        = number
  default     = 1
}

variable "shard_one_app_name" {
  description = "mongodb app name"
  type        = string
  default     = "shard-one"
}

variable "shard_one_units" {
  description = "Node count"
  type        = number
  default     = 1
}

variable "shard_two_app_name" {
  description = "mongodb app name"
  type        = string
  default     = "shard-two"
}

variable "shard_two_units" {
  description = "Node count"
  type        = number
  default     = 1
}