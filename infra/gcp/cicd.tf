# ---------------------------------------------------------------------------
# Artifact Registry: the Docker registry for the gateway and usage-consumer
# images. `just up` builds and pushes here, `just down` purges it before
# destroy. (File kept the historical name cicd.tf; the GitHub Actions
# Workload Identity part was removed - CI/CD is intentionally not part of
# this repo's flow.)
# ---------------------------------------------------------------------------

resource "google_artifact_registry_repository" "images" {
  location      = var.region
  repository_id = "inference-guard"
  description   = "Gateway and usage-consumer images."
  format        = "DOCKER"
}

output "artifact_registry_url" {
  description = "Docker registry for all inference-guard images."
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/inference-guard"
}
