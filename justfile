# inference-guard task runner — same tasks as make.ps1, works on Windows, macOS, Linux.
# Windows recipes run in the built-in PowerShell. macOS/Linux recipes run in the built-in sh.
# The only install is the runner itself, once per machine:
#   winget install Casey.Just   (Windows)  |  brew install just   (macOS)  |  sudo apt install just   (Linux)

set shell := ["sh", "-c"]
set windows-shell := ["powershell.exe", "-NoLogo", "-NoProfile", "-Command"]

tf_dir := "infra/gcp"

# list all tasks
default:
    @just --list

# terraform init + validate
[unix]
tf-check:
    terraform -chdir={{tf_dir}} init && terraform -chdir={{tf_dir}} validate

[windows]
tf-check:
    terraform -chdir={{tf_dir}} init ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; terraform -chdir={{tf_dir}} validate ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

# terraform plan (free, review before spending)
[unix]
plan:
    terraform -chdir={{tf_dir}} plan

[windows]
plan:
    terraform -chdir={{tf_dir}} plan ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

# full GKE stack: provision, build+push images, Kafka, deploy, wait for rollouts
[unix]
up:
    echo "== terraform: provision (keeps what exists)" && \
    terraform -chdir={{tf_dir}} init && terraform -chdir={{tf_dir}} apply -auto-approve && \
    AR=$(terraform -chdir={{tf_dir}} output -raw artifact_registry_url) && \
    CLUSTER=$(terraform -chdir={{tf_dir}} output -raw cluster_name) && \
    ZONE=$(terraform -chdir={{tf_dir}} output -raw zone) && \
    PROJECT=$(terraform -chdir={{tf_dir}} output -raw project_id) && \
    echo "== kubeconfig: $CLUSTER ($ZONE)" && \
    gcloud container clusters get-credentials "$CLUSTER" --zone "$ZONE" --project "$PROJECT" && \
    echo "== docker auth for ${AR%%/*}" && \
    gcloud auth configure-docker "${AR%%/*}" --quiet && \
    TAG=$(date +%Y%m%d-%H%M%S) && \
    echo "== build + push gateway:$TAG" && \
    docker build -f Dockerfile -t "$AR/gateway:$TAG" . && docker push "$AR/gateway:$TAG" && \
    echo "== build + push usage-consumer:$TAG" && \
    docker build -f Dockerfile.usage-consumer -t "$AR/usage-consumer:$TAG" . && docker push "$AR/usage-consumer:$TAG" && \
    echo "== namespaces" && \
    for ns in inference-guard kafka monitoring; do kubectl create namespace "$ns" --dry-run=client -o yaml | kubectl apply -f - >/dev/null; done && \
    echo "== gateway api-keys secret" && \
    kubectl -n inference-guard create secret generic gateway-api-keys --from-literal='api-keys=dev-key-a=tenant-a,dev-key-b=tenant-b' --dry-run=client -o yaml | kubectl apply -f - && \
    NV=$(kubectl get nodes -o jsonpath='{.items[0].status.nodeInfo.kubeletVersion}') && \
    MAJOR=$(printf '%s' "$NV" | sed -E 's/^v([0-9]+)\.([0-9]+).*/\1/') && \
    MINOR=$(printf '%s' "$NV" | sed -E 's/^v([0-9]+)\.([0-9]+).*/\2/') && \
    echo "== strimzi operator 0.43.0 (k8s $MAJOR.$MINOR auto-detected)" && \
    STRIMZI_KUBERNETES_VERSION="major=$MAJOR,minor=$MINOR" helm upgrade --install strimzi strimzi-kafka-operator --repo https://strimzi.io/charts/ --version 0.43.0 -n kafka --create-namespace && \
    echo "== kafka usage-kafka + topic inference-usage" && \
    kubectl apply -k deploy/kafka && \
    kubectl -n kafka wait kafka/usage-kafka --for=condition=Ready --timeout=600s && \
    kubectl -n kafka wait kafkatopic/inference-usage --for=condition=Ready --timeout=120s && \
    echo "== redis + usage-consumer" && \
    kubectl apply -k deploy/metering && \
    echo "== vllm (GPU) + gateway" && \
    kubectl apply -k deploy/base && kubectl apply -k deploy/gateway && \
    echo "== pin images to $TAG" && \
    kubectl -n inference-guard set image deployment/gateway gateway="$AR/gateway:$TAG" && \
    kubectl -n inference-guard set image deployment/usage-consumer usage-consumer="$AR/usage-consumer:$TAG" && \
    echo "== rollouts (first vLLM model pull can take 10-25 min)" && \
    kubectl -n inference-guard rollout status deployment/vllm --timeout=1800s && \
    kubectl -n inference-guard rollout status deployment/gateway --timeout=300s && \
    kubectl -n inference-guard rollout status deployment/usage-consumer --timeout=300s && \
    kubectl -n inference-guard get pods && \
    echo "GKE stack is up. Next: just gateway-smoke"

[windows]
up:
    echo '== terraform: provision (keeps what exists)' ; \
    terraform -chdir={{tf_dir}} init ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    terraform -chdir={{tf_dir}} apply -auto-approve ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    $AR = (terraform -chdir={{tf_dir}} output -raw artifact_registry_url) ; \
    $Cluster = (terraform -chdir={{tf_dir}} output -raw cluster_name) ; \
    $Zone = (terraform -chdir={{tf_dir}} output -raw zone) ; \
    $Project = (terraform -chdir={{tf_dir}} output -raw project_id) ; \
    if (-not $AR -or -not $Cluster -or -not $Zone) { echo 'ERROR: terraform outputs missing' ; exit 1 } ; \
    echo ('== kubeconfig: ' + $Cluster + ' (' + $Zone + ')') ; \
    gcloud container clusters get-credentials $Cluster --zone $Zone --project $Project ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    $RegHost = ($AR -split '/')[0] ; \
    echo ('== docker auth for ' + $RegHost) ; \
    gcloud auth configure-docker $RegHost --quiet ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    $Tag = Get-Date -Format 'yyyyMMdd-HHmmss' ; \
    echo ('== build + push gateway:' + $Tag) ; \
    docker build -f Dockerfile -t ($AR + '/gateway:' + $Tag) . ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    docker push ($AR + '/gateway:' + $Tag) ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    echo ('== build + push usage-consumer:' + $Tag) ; \
    docker build -f Dockerfile.usage-consumer -t ($AR + '/usage-consumer:' + $Tag) . ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    docker push ($AR + '/usage-consumer:' + $Tag) ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    echo '== namespaces' ; \
    foreach ($ns in 'inference-guard','kafka','monitoring') { kubectl create namespace $ns --dry-run=client -o yaml | kubectl apply -f - > $null ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } } ; \
    echo '== gateway api-keys secret' ; \
    kubectl -n inference-guard create secret generic gateway-api-keys --from-literal='api-keys=dev-key-a=tenant-a,dev-key-b=tenant-b' --dry-run=client -o yaml | kubectl apply -f - ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    $NV = (kubectl get nodes -o jsonpath='{.items[0].status.nodeInfo.kubeletVersion}') ; \
    $NV -match '^v([0-9]+)\.([0-9]+)' > $null ; \
    $Major = $Matches[1] ; $Minor = $Matches[2] ; \
    echo ('== strimzi operator 0.43.0 (k8s ' + $Major + '.' + $Minor + ' auto-detected)') ; \
    $env:STRIMZI_KUBERNETES_VERSION = 'major=' + $Major + ',minor=' + $Minor ; \
    helm upgrade --install strimzi strimzi-kafka-operator --repo https://strimzi.io/charts/ --version 0.43.0 -n kafka --create-namespace ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    echo '== kafka usage-kafka + topic inference-usage' ; \
    kubectl apply -k deploy/kafka ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    kubectl -n kafka wait kafka/usage-kafka --for=condition=Ready --timeout=600s ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    kubectl -n kafka wait kafkatopic/inference-usage --for=condition=Ready --timeout=120s ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    echo '== redis + usage-consumer' ; \
    kubectl apply -k deploy/metering ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    echo '== vllm (GPU) + gateway' ; \
    kubectl apply -k deploy/base ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    kubectl apply -k deploy/gateway ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    echo ('== pin images to ' + $Tag) ; \
    $ImgGW = 'gateway=' + $AR + '/gateway:' + $Tag ; \
    $ImgUC = 'usage-consumer=' + $AR + '/usage-consumer:' + $Tag ; \
    kubectl -n inference-guard set image deployment/gateway $ImgGW ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    kubectl -n inference-guard set image deployment/usage-consumer $ImgUC ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    echo '== rollouts (first vLLM model pull can take 10-25 min)' ; \
    kubectl -n inference-guard rollout status deployment/vllm --timeout=1800s ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    kubectl -n inference-guard rollout status deployment/gateway --timeout=300s ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    kubectl -n inference-guard rollout status deployment/usage-consumer --timeout=300s ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    kubectl -n inference-guard get pods ; \
    echo 'GKE stack is up. Next: just gateway-smoke'

# end-to-end proof: health, 401 without key, streamed 200 with tenant attribution
[unix]
gateway-smoke:
    GP=$(kubectl -n inference-guard get svc gateway -o jsonpath='{.spec.ports[0].port}') || { echo "ERROR: cannot read gateway service - run: just up" ; exit 1 ; } ; P=18080 ; \
    echo "== port-forward localhost:$P -> svc/gateway:$GP" ; \
    kubectl -n inference-guard port-forward svc/gateway "$P:$GP" >/dev/null 2>&1 & PF=$! ; sleep 3 ; \
    echo "Health: $(curl -s -m 5 http://localhost:$P/healthz)" ; \
    CODE=$(curl -s -o /dev/null -w '%{http_code}' -m 5 http://localhost:$P/v1/chat/completions) ; \
    echo "No-key request -> $CODE (expect 401)" ; \
    echo "== streaming one chat completion (tenant-a)" ; \
    curl -s -N -m 60 http://localhost:$P/v1/chat/completions -H "Authorization: Bearer dev-key-a" -H "Content-Type: application/json" -d '{"model":"Qwen/Qwen3-8B","messages":[{"role":"user","content":"Say OK"}],"max_tokens":5}' > /tmp/ig-smoke.sse ; \
    grep -q "data:" /tmp/ig-smoke.sse && echo "stream OK (SSE chunks received)" || { echo "STREAM FAILED" ; cat /tmp/ig-smoke.sse ; kill $PF 2>/dev/null ; exit 1 ; } ; \
    echo "== gateway metrics" ; \
    curl -s -m 5 http://localhost:$P/metrics | grep -E "^gateway_(requests_total|requests_in_flight|auth_rejections_total)" || true ; \
    kill $PF 2>/dev/null || true

[windows]
gateway-smoke:
    $GP = (kubectl -n inference-guard get svc gateway -o jsonpath='{.spec.ports[0].port}') ; if ($LASTEXITCODE -ne 0) { echo 'ERROR: cannot read gateway service - run: just up' ; exit 1 } ; \
    $P = '18080' ; \
    echo ('== port-forward localhost:' + $P + ' -> svc/gateway:' + $GP) ; \
    $job = Start-Job -ArgumentList $GP -ScriptBlock { param($gp) kubectl -n inference-guard port-forward svc/gateway ('18080:' + $gp) } ; \
    Start-Sleep -Seconds 3 ; \
    $ok = $false ; \
    foreach ($i in 1..20) { try { Invoke-WebRequest -Uri ('http://localhost:' + $P + '/healthz') -TimeoutSec 2 -UseBasicParsing > $null ; $ok = $true ; break } catch { Start-Sleep -Milliseconds 500 } } ; \
    if (-not $ok) { Stop-Job $job -ErrorAction SilentlyContinue ; Remove-Job $job -Force -ErrorAction SilentlyContinue ; echo 'ERROR: gateway not reachable - run: just up' ; exit 1 } ; \
    echo ('Health: ' + (Invoke-WebRequest -Uri ('http://localhost:' + $P + '/healthz') -UseBasicParsing).Content) ; \
    $Code = (curl.exe -s -o NUL -w '%{http_code}' -m 5 ('http://localhost:' + $P + '/v1/chat/completions')) ; \
    echo ('No-key request -> ' + $Code + ' (expect 401)') ; \
    echo '== streaming one chat completion (tenant-a)' ; \
    $Body = '{"model":"Qwen/Qwen3-8B","messages":[{"role":"user","content":"Say OK"}],"max_tokens":5}' ; \
    $Out = Join-Path $env:TEMP 'ig-smoke.sse' ; \
    curl.exe -s -N -m 60 ('http://localhost:' + $P + '/v1/chat/completions') -H 'Authorization: Bearer dev-key-a' -H 'Content-Type: application/json' -d $Body > $Out ; \
    if (-not (Select-String -Path $Out -Pattern 'data:' -Quiet)) { Get-Content $Out ; echo 'STREAM FAILED' ; Stop-Job $job -ErrorAction SilentlyContinue ; Remove-Job $job -Force -ErrorAction SilentlyContinue ; exit 1 } ; \
    echo 'stream OK (SSE chunks received)' ; \
    echo '== gateway metrics' ; \
    (curl.exe -s -m 5 ('http://localhost:' + $P + '/metrics')) | Select-String -Pattern '^gateway_(requests_total|requests_in_flight|auth_rejections_total)' | ForEach-Object { echo $_ } ; \
    Stop-Job $job -ErrorAction SilentlyContinue ; \
    Remove-Job $job -Force -ErrorAction SilentlyContinue

alias smoke := gateway-smoke

# dashboards: prints the password, forwards localhost:3000, blocks until Ctrl-C
[unix]
grafana:
    GPASS=$(kubectl -n monitoring get secret kube-prometheus-stack-grafana -o jsonpath='{.data.admin-password}' | openssl base64 -d -A 2>/dev/null) ; \
    if [ -z "$GPASS" ]; then echo "ERROR: could not read Grafana password - run: just up" ; exit 1 ; fi ; \
    GPORT=$(kubectl -n monitoring get svc kube-prometheus-stack-grafana -o jsonpath='{.spec.ports[0].port}') || exit 1 ; \
    echo "Grafana password: $GPASS" ; \
    echo "Open http://localhost:3000  (Ctrl-C stops the forward)" ; \
    kubectl -n monitoring port-forward svc/kube-prometheus-stack-grafana "3000:$GPORT"

[windows]
grafana:
    $pwRaw = (kubectl -n monitoring get secret kube-prometheus-stack-grafana -o jsonpath='{.data.admin-password}') ; \
    if (-not $pwRaw) { echo 'ERROR: could not read Grafana password - run: just up' ; exit 1 } ; \
    $GPASS = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($pwRaw)) ; \
    $GPORT = (kubectl -n monitoring get svc kube-prometheus-stack-grafana -o jsonpath='{.spec.ports[0].port}') ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    echo ('Grafana password: ' + $GPASS) ; \
    echo 'Open http://localhost:3000  (Ctrl-C stops the forward)' ; \
    kubectl -n monitoring port-forward svc/kube-prometheus-stack-grafana ('3000:' + $GPORT)

# destroy everything; purges AR images first so destroy stays clean
[unix]
down:
    echo "== artifact registry purge" ; \
    AR=$(terraform -chdir={{tf_dir}} output -raw artifact_registry_url 2>/dev/null) || AR="" ; \
    if [ -n "$AR" ]; then \
      REGHOST=${AR%%/*} ; REPO=${AR##*/} ; LOC=${REGHOST%%-docker.pkg.dev} ; \
      PROJ=$(printf '%s' "$AR" | sed -E 's#.*pkg\.dev/([^/]+)/.*#\1#') ; \
      echo "== deleting $REPO ($LOC, project $PROJ)" ; \
      gcloud artifacts repositories delete "$REPO" --location="$LOC" --project="$PROJ" --quiet ; \
    fi ; \
    echo "== terraform destroy" ; \
    terraform -chdir={{tf_dir}} destroy -auto-approve

[windows]
down:
    echo '== artifact registry purge' ; \
    $AR = (terraform -chdir={{tf_dir}} output -raw artifact_registry_url 2>$null) ; \
    if ($AR) { \
      $RegHost = ($AR -split '/')[0] ; $Repo = ($AR -split '/')[-1] ; $Loc = ($RegHost -replace '-docker\.pkg\.dev$', '') ; $Proj = ($AR -split '/')[1] ; \
      echo ('== deleting ' + $Repo + ' (' + $Loc + ', project ' + $Proj + ')') ; \
      gcloud artifacts repositories delete $Repo --location=$Loc --project=$Proj --quiet ; \
    } ; \
    echo '== terraform destroy' ; \
    terraform -chdir={{tf_dir}} destroy -auto-approve ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

# local kind stack (CPU-sized): strimzi operator + manifests + api keys
[unix]
local-up:
    kubectl create namespace kafka --dry-run=client -o yaml | kubectl apply -f - && \
    kubectl apply -f "https://strimzi.io/install/latest?namespace=kafka" -n kafka && \
    kubectl apply -k deploy/local && \
    kubectl -n inference-guard create secret generic gateway-api-keys --from-literal="api-keys=dev-key-a=tenant-a,dev-key-b=tenant-b" --dry-run=client -o yaml | kubectl apply -f - && \
    echo "Local stack applied. Next: just gateway-smoke (with the kind context active)"

[windows]
local-up:
    kubectl create namespace kafka --dry-run=client -o yaml | kubectl apply -f - ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    kubectl apply -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    kubectl apply -k deploy/local ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    kubectl -n inference-guard create secret generic gateway-api-keys --from-literal='api-keys=dev-key-a=tenant-a,dev-key-b=tenant-b' --dry-run=client -o yaml | kubectl apply -f - ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    echo 'Local stack applied. Next: just gateway-smoke (with the kind context active)'