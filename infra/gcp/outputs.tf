output "project_id" {
  description = "GCP project id."
  value       = var.project_id
}

output "cluster_name" {
  description = "GKE cluster name."
  value       = google_container_cluster.primary.name
}

output "zone" {
  description = "Zone the cluster lives in."
  value       = var.zone
}

output "region" {
  description = "Region."
  value       = var.region
}
