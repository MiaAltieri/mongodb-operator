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
