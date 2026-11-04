# ---------------------------------------------------------------------------
# Phase 3 CI/CD: Artifact Registry + GitHub Actions Workload Identity
# Federation. GitHub Actions pushes images without any long-lived key.
# ---------------------------------------------------------------------------

variable "github_org" {
  description = "GitHub user or organization that owns the repository (change if different)."
  type        = string
  default     = "nynrathod"
}

variable "github_repo" {
  description = "GitHub repository name (change if different)."
  type        = string
  default     = "inference-guard"
}

resource "google_artifact_registry_repository" "images" {
  location      = var.region
  repository_id = "inference-guard"
  description   = "Gateway, usage-consumer and pre-baked vLLM images."
  format        = "DOCKER"
}

resource "google_iam_workload_identity_pool" "github" {
  workload_identity_pool_id = "github-actions"
  display_name              = "GitHub Actions"
  description               = "inference-guard repository CI."
}

resource "google_iam_workload_identity_pool_provider" "github" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github-oidc"
  display_name                       = "GitHub OIDC"
  description                        = "Trusts OIDC tokens issued by GitHub Actions."

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
  }
  attribute_condition = "assertion.repository == \"${var.github_org}/${var.github_repo}\""

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

resource "google_service_account" "github_actions" {
  account_id   = "gh-inference-guard"
  display_name = "GitHub Actions (inference-guard)"
  description  = "Used only by GitHub Actions via WIF to push images."
}

resource "google_project_iam_member" "images_writer" {
  project = var.project_id
  role    = "roles/artifactregistry.writer"
  member  = "serviceAccount:${google_service_account.github_actions.email}"
}

resource "google_service_account_iam_member" "github_impersonation" {
  service_account_id = google_service_account.github_actions.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/attribute.repository/${var.github_org}/${var.github_repo}"
}

output "artifact_registry_url" {
  description = "Docker registry for all inference-guard images."
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/inference-guard"
}

output "github_actions_wif_provider" {
  description = "workload_identity_provider value for google-github-actions/auth."
  value       = google_iam_workload_identity_pool_provider.github.name
}

output "github_actions_service_account" {
  description = "service_account email for google-github-actions/auth."
  value       = google_service_account.github_actions.email
}
