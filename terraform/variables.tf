# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

variable "app_name" {
  description = "Application name"
  type        = string
  default     = "mongodb"
}

variable "channel" {
  description = "Charm channel"
  type        = string
  default     = "6/stable"
}



variable "config" {
  description = "Map of charm configuration options"
  type        = map(string)
  default     = {}
}

variable "model" {
  description = "Model name"
  type        = string
}

variable "revision" {
  description = "Charm revision"
  type        = number
  default     = null
}

variable "units" {
  description = "Charm units"
  type        = number
  default     = 3
}

variable "constraints" {
  description = "String listing constraints for this application"
  type        = string
  default     = "arch=amd64"
}

variable "machines" {
  description = "List of machines for placement"
  type        = list(string)
  default     = []
}

variable "storage" {
  description = "Map of storage used by the application"
  type        = map(string)
  default     = {}
}

variable "endpoint_bindings" {
  description = "Map of endpoint bindings"
  type        = map(string)
  default     = {}
}