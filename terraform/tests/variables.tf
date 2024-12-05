variable "model" {
  description = "Model name"
  type        = string
}

variable "app_name" {
  description = "mongodb app name"
  type        = string
  default     = "mongodb"
}

variable "simple_mongodb_units" {
  description = "Node count"
  type        = number
  default     = 1
}
