terraform {
  required_version = ">= 1.6.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.44"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.16"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

data "google_client_config" "this" {}

data "google_project" "this" {
  project_id = var.project_id
}

resource "google_container_cluster" "primary" {
  name     = var.cluster_name
  location = var.zone

  deletion_protection = false

  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }

  release_channel {
    channel = "REGULAR"
  }

  remove_default_node_pool = true
  initial_node_count       = 1

  lifecycle {
    ignore_changes = [node_config]
  }
}

resource "google_container_node_pool" "cpu" {
  name       = "cpu"
  cluster    = google_container_cluster.primary.id
  location   = var.zone
  node_count = 1

  node_config {
    machine_type = "e2-standard-4"
    oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }
}

resource "google_container_node_pool" "gpu_spot" {
  name       = "gpu-spot"
  cluster    = google_container_cluster.primary.id
  location   = var.zone
  node_count = 1

  node_config {
    machine_type = "g2-standard-4"
    spot         = true
    oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]

    guest_accelerator {
      type  = "nvidia-l4"
      count = 1
    }

    taint {
      key    = "nvidia.com/gpu"
      value  = "present"
      effect = "NO_SCHEDULE"
    }
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }
}


resource "google_billing_budget" "project" {
  count = var.create_budget && var.billing_account != "" ? 1 : 0

  billing_account = var.billing_account
  display_name    = "inference-guard monthly budget"

  budget_filter {
    projects = ["projects/${data.google_project.this.number}"]
  }

  amount {
    specified_amount {
      currency_code = "USD"
      units         = tostring(var.budget_amount)
    }
  }

  threshold_rules {
    threshold_percent = 0.5
  }
  threshold_rules {
    threshold_percent = 1.0
  }
}

provider "helm" {
  kubernetes {
    host                   = "https://${google_container_cluster.primary.endpoint}"
    token                  = data.google_client_config.this.access_token
    cluster_ca_certificate = base64decode(google_container_cluster.primary.master_auth[0].cluster_ca_certificate)
  }
}

resource "helm_release" "kube_prometheus_stack" {
  name             = "kube-prometheus-stack"
  repository       = "https://prometheus-community.github.io/helm-charts"
  chart            = "kube-prometheus-stack"
  namespace        = "monitoring"
  create_namespace = true
  version          = var.kube_prometheus_stack_version
  wait             = true
  timeout          = 600

  values = [
    yamlencode({
      prometheus = {
        prometheusSpec = {
          podMonitorSelectorNilUsesHelmValues = false
          retention                           = "2d"
        }
      }
      alertmanager = {
        enabled = false
      }
    })
  ]
}
