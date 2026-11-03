variable "project_id" {
  description = "GCP project id (auto-filled by scripts/setup.sh — never edit by hand)."
  type        = string
}

variable "region" {
  description = "GCP region."
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "GCP zone for the zonal cluster and node pools. Try us-central1-f or us-central1-b if L4 Spot capacity is tight."
  type        = string
  default     = "us-central1-a"
}

variable "cluster_name" {
  description = "GKE cluster name."
  type        = string
  default     = "inference-guard"
}

variable "billing_account" {
  description = "Billing account id (auto-filled by scripts/setup.sh; only used when create_budget = true)."
  type        = string
  default     = ""
}

variable "create_budget" {
  description = "Create a monthly budget alert (opt-in: make setup BUDGET=1)."
  type        = bool
  default     = false
}

variable "budget_amount" {
  description = "Monthly budget in USD for the alert."
  type        = number
  default     = 25
}

variable "kube_prometheus_stack_version" {
  description = "kube-prometheus-stack chart version, pinned for reproducibility. Current: https://artifacthub.io/packages/helm/prometheus-community/kube-prometheus-stack"
  type        = string
  default     = "92.16.1"
}
