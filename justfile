# inference-guard task runner — works on Windows, macOS, Linux.
# Windows recipes run in the built-in PowerShell. macOS/Linux recipes run in the built-in sh.
# The only install is the runner itself, once per machine:
#   winget install Casey.Just   (Windows)  |  brew install just   (macOS)  |  sudo apt install just   (Linux)
# The leading @ on recipes is just's "don't echo the command" prefix - output and
# phase markers still print, the command text does not.

set shell := ["sh", "-c"]
set windows-shell := ["powershell.exe", "-NoLogo", "-NoProfile", "-Command"]

tf_dir := "infra/gcp"
calibration_scenario := "bench/scenarios/02-saturation.json"

# list all tasks
default:
    @just --list

# terraform init + validate
[unix]
tf-check:
    @terraform -chdir={{tf_dir}} init && terraform -chdir={{tf_dir}} validate

[windows]
tf-check:
    @terraform -chdir={{tf_dir}} init ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; terraform -chdir={{tf_dir}} validate ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

# terraform plan (free, review before spending)
[unix]
plan:
    @terraform -chdir={{tf_dir}} plan

[windows]
plan:
    @terraform -chdir={{tf_dir}} plan ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

# full GKE stack: provision, build+push images, Kafka, deploy, wait for rollouts
[unix]
up:
    @echo "== docker: engine check" && \
    docker info >/dev/null 2>&1 || { echo "ERROR: docker daemon not reachable - start it and re-run: just up" ; exit 1 ; } && \
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
    docker build -f Dockerfile -t "$AR/gateway:$TAG" . && \
    pushed=0 ; for t in 1 2 3 4 5; do if docker push "$AR/gateway:$TAG" ; then pushed=1 ; break ; fi ; echo "== push failed (attempt $t/5) - transient network/DNS - retrying in 20s" ; sleep 20 ; done ; \
    [ "$pushed" = 1 ] || { echo "ERROR: docker push kept failing - docker cannot reach the registry, check the network and re-run: just up" ; exit 1 ; } && \
    echo "== build + push usage-consumer:$TAG" && \
    docker build -f Dockerfile.usage-consumer -t "$AR/usage-consumer:$TAG" . && \
    pushed=0 ; for t in 1 2 3 4 5; do if docker push "$AR/usage-consumer:$TAG" ; then pushed=1 ; break ; fi ; echo "== push failed (attempt $t/5) - transient network/DNS - retrying in 20s" ; sleep 20 ; done ; \
    [ "$pushed" = 1 ] || { echo "ERROR: docker push kept failing - docker cannot reach the registry, check the network and re-run: just up" ; exit 1 ; } && \
    echo "== namespaces" && \
    for ns in inference-guard kafka monitoring; do kubectl create namespace "$ns" --dry-run=client -o yaml | kubectl apply -f - >/dev/null; done && \
    echo "== gateway api-keys secret" && \
    kubectl -n inference-guard create secret generic gateway-api-keys --from-literal='api-keys=dev-key-a=tenant-a,dev-key-b=tenant-b' --dry-run=client -o yaml | kubectl apply -f - && \
    NV=$(kubectl get nodes -o jsonpath='{.items[0].status.nodeInfo.kubeletVersion}') && \
    MAJOR=$(printf '%s' "$NV" | sed -E 's/^v([0-9]+)\.([0-9]+).*/\1/') && \
    MINOR=$(printf '%s' "$NV" | sed -E 's/^v([0-9]+)\.([0-9]+).*/\2/') && \
    if [ -z "$MAJOR" ] || [ -z "$MINOR" ] ; then echo "ERROR: could not auto-detect k8s version from kubelet '$NV'" ; exit 1 ; fi && \
    echo "== strimzi operator 0.43.0 (k8s $MAJOR.$MINOR auto-detected)" && \
    helm upgrade --install strimzi strimzi-kafka-operator --repo https://strimzi.io/charts/ --version 0.43.0 -n kafka --create-namespace && \
    echo "== pin STRIMZI_KUBERNETES_VERSION on the operator (k8s 1.33+ fabric8 version-decode fix)" && \
    kubectl -n kafka set env deployment/strimzi-cluster-operator "STRIMZI_KUBERNETES_VERSION=major=$MAJOR,minor=$MINOR" && \
    echo "== kafka usage-kafka + topic inference-usage" && \
    kubectl apply -k deploy/kafka && \
    echo "== waiting for strimzi operator to be up" && \
    kubectl -n kafka rollout status deployment/strimzi-cluster-operator --timeout=420s || \
    { echo "== operator rollout stuck - re-pinning k8s version env + restarting once (auto-recovery)" ; \
      kubectl -n kafka set env deployment/strimzi-cluster-operator "STRIMZI_KUBERNETES_VERSION=major=$MAJOR,minor=$MINOR" ; \
      kubectl -n kafka rollout restart deployment/strimzi-cluster-operator ; \
      kubectl -n kafka rollout status deployment/strimzi-cluster-operator --timeout=420s ; } ; \
    if [ $? -ne 0 ]; then \
      echo "== strimzi operator did not come up - full diagnosis below, paste from this marker:" ; \
      kubectl -n kafka get pods -o wide ; \
      echo "== operator deployment conditions:" ; \
      kubectl -n kafka describe deployment/strimzi-cluster-operator ; \
      echo "== recent events:" ; \
      kubectl -n kafka get events --sort-by=.lastTimestamp | tail -25 ; \
      echo "== operator log - root error lines:" ; \
      kubectl -n kafka logs deployment/strimzi-cluster-operator --tail=400 2>/dev/null | grep -E 'ERROR|Caused by|Exception|Failed to' | head -15 ; \
      echo "== operator log (last 50 lines):" ; \
      kubectl -n kafka logs deployment/strimzi-cluster-operator --tail=50 ; \
      exit 1 ; \
    fi ; \
    echo "== waiting for kafka to become Ready (up to 15 min, heartbeat every 30s)" && \
    OK=0 && for i in $(seq 1 30); do \
      if kubectl -n kafka wait kafka/usage-kafka --for=condition=Ready --timeout=30s >/dev/null 2>&1 ; then OK=1 ; break ; fi ; \
      echo "  [$((i*30))s] kafka still starting... pods: $(kubectl -n kafka get pods --no-headers 2>/dev/null | awk '{print $1"="$3}' | tr '\n' ' ')" ; \
      sleep 2 ; \
    done ; \
    if [ "$OK" != "1" ]; then \
      echo "== kafka did not become Ready in 15 min - full diagnosis below, paste from this marker:" ; \
      kubectl -n kafka get pods -o wide ; \
      echo "== kafka status conditions:" ; \
      kubectl -n kafka get kafka usage-kafka -o yaml | sed -n '/conditions:/,$p' ; \
      echo "== recent events:" ; \
      kubectl -n kafka get events --sort-by=.lastTimestamp | tail -25 ; \
      echo "== strimzi operator log - root error lines:" ; \
      kubectl -n kafka logs deployment/strimzi-cluster-operator --tail=400 2>/dev/null | grep -E 'ERROR|Caused by|Exception|Failed to' | head -15 ; \
      echo "== strimzi operator log (last 30 lines):" ; \
      kubectl -n kafka logs deployment/strimzi-cluster-operator --tail=30 ; \
      exit 1 ; \
    fi ; \
    echo "== kafka topic inference-usage" && \
    kubectl -n kafka wait kafkatopic/inference-usage --for=condition=Ready --timeout=300s && \
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
    @echo '== docker: engine check' ; \
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { echo 'ERROR: docker CLI not found - install Docker Desktop first: https://docs.docker.com/desktop/setup/install/windows-install/' ; exit 1 } ; \
    docker info > $null 2>$null ; \
    if ($LASTEXITCODE -ne 0) { \
      echo '== docker engine not running - starting Docker Desktop (first start can take 1-3 min)' ; \
      $dd = 'C:\Program Files\Docker\Docker\Docker Desktop.exe' ; \
      if (Test-Path $dd) { Start-Process $dd } ; \
      $dockerOk = $false ; \
      foreach ($i in 1..60) { \
        Start-Sleep -Seconds 5 ; \
        docker info > $null 2>$null ; \
        if ($LASTEXITCODE -eq 0) { $dockerOk = $true ; break } ; \
        if ($i % 6 -eq 0) { echo ('  [' + ($i * 5) + 's] still waiting for the docker engine...') } ; \
      } ; \
      if (-not $dockerOk) { echo 'ERROR: docker engine still unreachable - open Docker Desktop, wait for the green engine status, then re-run: just up' ; exit 1 } ; \
    } ; \
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
    $pushed = $false ; \
    foreach ($try in 1..5) { \
      docker push ($AR + '/gateway:' + $Tag) ; \
      if ($LASTEXITCODE -eq 0) { $pushed = $true ; break } ; \
      echo ('== push failed (attempt ' + $try + '/5) - transient network/DNS - retrying in 20s') ; \
      Start-Sleep -Seconds 20 ; \
    } ; \
    if (-not $pushed) { echo 'ERROR: docker push kept failing - the Docker VM cannot reach the registry. Fix: wsl --shutdown, reopen Docker Desktop, wait for the green engine, then re-run: just up' ; exit 1 } ; \
    echo ('== build + push usage-consumer:' + $Tag) ; \
    docker build -f Dockerfile.usage-consumer -t ($AR + '/usage-consumer:' + $Tag) . ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    $pushed = $false ; \
    foreach ($try in 1..5) { \
      docker push ($AR + '/usage-consumer:' + $Tag) ; \
      if ($LASTEXITCODE -eq 0) { $pushed = $true ; break } ; \
      echo ('== push failed (attempt ' + $try + '/5) - transient network/DNS - retrying in 20s') ; \
      Start-Sleep -Seconds 20 ; \
    } ; \
    if (-not $pushed) { echo 'ERROR: docker push kept failing - the Docker VM cannot reach the registry. Fix: wsl --shutdown, reopen Docker Desktop, wait for the green engine, then re-run: just up' ; exit 1 } ; \
    echo '== namespaces' ; \
    foreach ($ns in 'inference-guard','kafka','monitoring') { kubectl create namespace $ns --dry-run=client -o yaml | kubectl apply -f - > $null ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } } ; \
    echo '== gateway api-keys secret' ; \
    kubectl -n inference-guard create secret generic gateway-api-keys --from-literal='api-keys=dev-key-a=tenant-a,dev-key-b=tenant-b' --dry-run=client -o yaml | kubectl apply -f - ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    $NV = (kubectl get nodes -o jsonpath='{.items[0].status.nodeInfo.kubeletVersion}') ; \
    $NV -match '^v([0-9]+)\.([0-9]+)' > $null ; \
    $Major = $Matches[1] ; $Minor = $Matches[2] ; \
    if (-not $Major -or -not $Minor) { echo ('ERROR: could not auto-detect k8s version from kubelet ' + $NV) ; exit 1 } ; \
    echo ('== strimzi operator 0.43.0 (k8s ' + $Major + '.' + $Minor + ' auto-detected)') ; \
    helm upgrade --install strimzi strimzi-kafka-operator --repo https://strimzi.io/charts/ --version 0.43.0 -n kafka --create-namespace ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    echo ('== pin STRIMZI_KUBERNETES_VERSION on the operator (k8s 1.33+ fabric8 version-decode fix) major=' + $Major + ',minor=' + $Minor) ; \
    kubectl -n kafka set env deployment/strimzi-cluster-operator ('STRIMZI_KUBERNETES_VERSION=major=' + $Major + ',minor=' + $Minor) ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    echo '== kafka usage-kafka + topic inference-usage' ; \
    kubectl apply -k deploy/kafka ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    echo '== waiting for strimzi operator to be up' ; \
    kubectl -n kafka rollout status deployment/strimzi-cluster-operator --timeout=420s ; \
    if ($LASTEXITCODE -ne 0) { \
      echo '== operator rollout stuck - re-pinning k8s version env + restarting once (auto-recovery)' ; \
      kubectl -n kafka set env deployment/strimzi-cluster-operator ('STRIMZI_KUBERNETES_VERSION=major=' + $Major + ',minor=' + $Minor) ; \
      kubectl -n kafka rollout restart deployment/strimzi-cluster-operator ; \
      kubectl -n kafka rollout status deployment/strimzi-cluster-operator --timeout=420s ; \
    } ; \
    if ($LASTEXITCODE -ne 0) { \
      echo '== strimzi operator did not come up - full diagnosis below, paste from this marker:' ; \
      kubectl -n kafka get pods -o wide ; \
      echo '== operator deployment conditions:' ; \
      kubectl -n kafka describe deployment/strimzi-cluster-operator ; \
      echo '== recent events:' ; \
      kubectl -n kafka get events --sort-by=.lastTimestamp | Select-Object -Last 25 ; \
      echo '== operator log - root error lines:' ; \
      kubectl -n kafka logs deployment/strimzi-cluster-operator --tail=400 2>$null | Select-String -Pattern 'ERROR|Caused by|Exception|Failed to' | Select-Object -First 15 | ForEach-Object { $_.Line } ; \
      echo '== operator log (last 50 lines):' ; \
      kubectl -n kafka logs deployment/strimzi-cluster-operator --tail=50 ; \
      exit 1 ; \
    } ; \
    echo '== waiting for kafka to become Ready (up to 15 min, heartbeat every 30s)' ; \
    $kafkaOk = $false ; \
    foreach ($i in 1..30) { \
      kubectl -n kafka wait kafka/usage-kafka --for=condition=Ready --timeout=30s > $null 2>$null ; \
      if ($LASTEXITCODE -eq 0) { $kafkaOk = $true ; break } ; \
      $pods = ((kubectl -n kafka get pods --no-headers 2>$null | ForEach-Object { $p = $_.Trim() -split '\s+' ; $p[0] + '=' + $p[2] }) -join ' ') ; \
      echo ('  [' + ($i * 30) + 's] kafka still starting... pods: ' + $pods) ; \
      Start-Sleep -Seconds 2 ; \
    } ; \
    if (-not $kafkaOk) { \
      echo '== kafka did not become Ready in 15 min - full diagnosis below, paste from this marker:' ; \
      kubectl -n kafka get pods -o wide ; \
      echo '== kafka status conditions:' ; \
      kubectl -n kafka get kafka usage-kafka -o yaml | Select-String -Pattern 'conditions:' -Context 0,30 ; \
      echo '== recent events:' ; \
      kubectl -n kafka get events --sort-by=.lastTimestamp | Select-Object -Last 25 ; \
      echo '== strimzi operator log - root error lines:' ; \
      kubectl -n kafka logs deployment/strimzi-cluster-operator --tail=400 2>$null | Select-String -Pattern 'ERROR|Caused by|Exception|Failed to' | Select-Object -First 15 | ForEach-Object { $_.Line } ; \
      echo '== strimzi operator log (last 30 lines):' ; \
      kubectl -n kafka logs deployment/strimzi-cluster-operator --tail=30 ; \
      exit 1 ; \
    } ; \
    echo '== kafka topic inference-usage' ; \
    kubectl -n kafka wait kafkatopic/inference-usage --for=condition=Ready --timeout=300s ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
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

# end-to-end proof: health, 401 without key, model discovery, streamed 200 with tenant attribution.
# Windows note: the JSON body is written to a temp file and sent with
# curl --data-binary @file, because PowerShell mangles inline quotes passed
# to native executables (that is what produced vLLM "json_invalid" 400s).
[unix]
gateway-smoke:
    @GP=$(kubectl -n inference-guard get svc gateway -o jsonpath='{.spec.ports[0].port}') || { echo "ERROR: cannot read gateway service - run: just up" ; exit 1 ; } ; P=18080 ; \
    echo "== port-forward localhost:$P -> svc/gateway:$GP" ; \
    kubectl -n inference-guard port-forward svc/gateway "$P:$GP" >/dev/null 2>&1 & PF=$! ; sleep 3 ; \
    echo "Health: $(curl -s -m 5 http://localhost:$P/healthz)" ; \
    CODE=$(curl -s -o /dev/null -w '%{http_code}' -m 5 http://localhost:$P/v1/chat/completions) ; \
    echo "No-key request -> $CODE (expect 401)" ; \
    MODEL=$(curl -s -m 5 http://localhost:$P/v1/models -H "Authorization: Bearer dev-key-a" | sed -E 's/.*"id":"([^"]+)".*/\1/' | head -1) ; \
    if [ -z "$MODEL" ]; then echo "ERROR: no model served - check the vllm pod" ; kill $PF 2>/dev/null ; exit 1 ; fi ; \
    echo "== served model: $MODEL" ; \
    echo "== streaming one chat completion (tenant-a)" ; \
    BODY=$(printf '{"model":"%s","messages":[{"role":"user","content":"Say OK"}],"max_tokens":5,"stream":true}' "$MODEL") ; \
    curl -s -N -m 60 http://localhost:$P/v1/chat/completions -H "Authorization: Bearer dev-key-a" -H "Content-Type: application/json" -d "$BODY" > /tmp/ig-smoke.sse ; \
    grep -q "data:" /tmp/ig-smoke.sse && echo "stream OK (SSE chunks received)" || { echo "STREAM FAILED" ; cat /tmp/ig-smoke.sse ; kill $PF 2>/dev/null ; exit 1 ; } ; \
    echo "== gateway metrics" ; \
    curl -s -m 5 http://localhost:$P/metrics | grep -E "^gateway_(requests_total|requests_in_flight|auth_rejections_total|time_to_first_token_seconds_count)" || true ; \
    kill $PF 2>/dev/null || true

[windows]
gateway-smoke:
    @$GP = (kubectl -n inference-guard get svc gateway -o jsonpath='{.spec.ports[0].port}') ; if ($LASTEXITCODE -ne 0) { echo 'ERROR: cannot read gateway service - run: just up' ; exit 1 } ; \
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
    $Models = (Invoke-WebRequest -Uri ('http://localhost:' + $P + '/v1/models') -Headers @{ Authorization = 'Bearer dev-key-a' } -UseBasicParsing).Content ; \
    $Model = (($Models | ConvertFrom-Json).data | Select-Object -First 1).id ; \
    if (-not $Model) { Stop-Job $job -ErrorAction SilentlyContinue ; Remove-Job $job -Force -ErrorAction SilentlyContinue ; echo 'ERROR: no model served - check the vllm pod' ; exit 1 } ; \
    echo ('== served model: ' + $Model) ; \
    echo '== streaming one chat completion (tenant-a)' ; \
    $BodyFile = Join-Path $env:TEMP 'ig-smoke-body.json' ; \
    $Body = '{"model":"' + $Model + '","messages":[{"role":"user","content":"Say OK"}],"max_tokens":5,"stream":true}' ; \
    Set-Content -Path $BodyFile -Value $Body -Encoding ascii ; \
    $Out = Join-Path $env:TEMP 'ig-smoke.sse' ; \
    curl.exe -s -N -m 60 ('http://localhost:' + $P + '/v1/chat/completions') -H 'Authorization: Bearer dev-key-a' -H 'Content-Type: application/json' --data-binary ('@' + $BodyFile) > $Out ; \
    if (-not (Select-String -Path $Out -Pattern 'data:' -Quiet)) { Get-Content $Out ; echo 'STREAM FAILED' ; Stop-Job $job -ErrorAction SilentlyContinue ; Remove-Job $job -Force -ErrorAction SilentlyContinue ; exit 1 } ; \
    echo 'stream OK (SSE chunks received)' ; \
    echo '== gateway metrics' ; \
    (curl.exe -s -m 5 ('http://localhost:' + $P + '/metrics')) | Select-String -Pattern '^gateway_(requests_total|requests_in_flight|auth_rejections_total|time_to_first_token_seconds_count)' | ForEach-Object { echo $_ } ; \
    Stop-Job $job -ErrorAction SilentlyContinue ; \
    Remove-Job $job -Force -ErrorAction SilentlyContinue

alias smoke := gateway-smoke

# sustained traffic so Grafana shows something: Prometheus scrapes every 30s,
# so a single request barely registers - this sends alternating tenant-a /
# tenant-b streaming requests (default 60, ~1s apart). Run it in one terminal
# and watch the dashboard in another (just grafana).
[unix]
gateway-load iterations='60':
    @GP=$(kubectl -n inference-guard get svc gateway -o jsonpath='{.spec.ports[0].port}') || { echo "ERROR: cannot read gateway service - run: just up" ; exit 1 ; } ; P=18080 ; \
    kubectl -n inference-guard port-forward svc/gateway "$P:$GP" >/dev/null 2>&1 & PF=$! ; sleep 3 ; \
    if ! curl -s -m 3 http://localhost:$P/healthz >/dev/null ; then echo "ERROR: gateway not reachable - run: just up" ; kill $PF 2>/dev/null ; exit 1 ; fi ; \
    MODEL=$(curl -s -m 5 http://localhost:$P/v1/models -H "Authorization: Bearer dev-key-a" | sed -E 's/.*"id":"([^"]+)".*/\1/' | head -1) ; \
    if [ -z "$MODEL" ]; then echo "ERROR: no model served - check the vllm pod" ; kill $PF 2>/dev/null ; exit 1 ; fi ; \
    BODY=$(printf '{"model":"%s","messages":[{"role":"user","content":"Say OK"}],"max_tokens":5,"stream":true}' "$MODEL") ; \
    echo "== sending {{iterations}} streaming requests, alternating tenant-a/tenant-b - keep this running while you watch Grafana" ; \
    OK=0 ; for i in $(seq 1 {{iterations}}) ; do \
      if [ $((i % 2)) -eq 0 ] ; then KEY=dev-key-a ; else KEY=dev-key-b ; fi ; \
      CODE=$(curl -s -o /dev/null -w '%{http_code}' -m 60 http://localhost:$P/v1/chat/completions -H "Authorization: Bearer $KEY" -H "Content-Type: application/json" -d "$BODY") ; \
      if [ "$CODE" = "200" ] ; then OK=$((OK+1)) ; fi ; \
      echo "  [$i/{{iterations}}] $KEY -> $CODE" ; sleep 1 ; \
    done ; \
    echo "== done: $OK/{{iterations}} requests returned 200 - give Prometheus 1-2 min of scrapes, then check Grafana" ; \
    kill $PF 2>/dev/null || true

[windows]
gateway-load iterations='60':
    @$GP = (kubectl -n inference-guard get svc gateway -o jsonpath='{.spec.ports[0].port}') ; if ($LASTEXITCODE -ne 0) { echo 'ERROR: cannot read gateway service - run: just up' ; exit 1 } ; \
    $P = '18080' ; \
    $job = Start-Job -ArgumentList $GP -ScriptBlock { param($gp) kubectl -n inference-guard port-forward svc/gateway ('18080:' + $gp) } ; \
    Start-Sleep -Seconds 3 ; \
    $up = $false ; \
    foreach ($i in 1..20) { try { Invoke-WebRequest -Uri ('http://localhost:' + $P + '/healthz') -TimeoutSec 2 -UseBasicParsing > $null ; $up = $true ; break } catch { Start-Sleep -Milliseconds 500 } } ; \
    if (-not $up) { Stop-Job $job -ErrorAction SilentlyContinue ; Remove-Job $job -Force -ErrorAction SilentlyContinue ; echo 'ERROR: gateway not reachable - run: just up' ; exit 1 } ; \
    $Models = (Invoke-WebRequest -Uri ('http://localhost:' + $P + '/v1/models') -Headers @{ Authorization = 'Bearer dev-key-a' } -UseBasicParsing).Content ; \
    $Model = (($Models | ConvertFrom-Json).data | Select-Object -First 1).id ; \
    if (-not $Model) { echo 'ERROR: no model served - check the vllm pod' ; Stop-Job $job -ErrorAction SilentlyContinue ; Remove-Job $job -Force -ErrorAction SilentlyContinue ; exit 1 } ; \
    $BodyFile = Join-Path $env:TEMP 'ig-load-body.json' ; \
    Set-Content -Path $BodyFile -Value ('{"model":"' + $Model + '","messages":[{"role":"user","content":"Say OK"}],"max_tokens":5,"stream":true}') -Encoding ascii ; \
    echo ('== sending {{iterations}} streaming requests, alternating tenant-a/tenant-b - keep this running while you watch Grafana') ; \
    $ok = 0 ; \
    foreach ($i in 1..{{iterations}}) { \
      if (($i % 2) -eq 0) { $Key = 'dev-key-a' } else { $Key = 'dev-key-b' } ; \
      $Code = (curl.exe -s -o NUL -w '%{http_code}' -m 60 ('http://localhost:' + $P + '/v1/chat/completions') -H ('Authorization: Bearer ' + $Key) -H 'Content-Type: application/json' --data-binary ('@' + $BodyFile)) ; \
      if ($Code -eq '200') { $ok = $ok + 1 } ; \
      echo ('  [' + $i + '/{{iterations}}] ' + $Key + ' -> ' + $Code) ; \
      Start-Sleep -Seconds 1 ; \
    } ; \
    echo ('== done: ' + $ok + '/{{iterations}} requests returned 200 - give Prometheus 1-2 min of scrapes, then check Grafana') ; \
    Stop-Job $job -ErrorAction SilentlyContinue ; \
    Remove-Job $job -Force -ErrorAction SilentlyContinue

# go vet + build + tests (race detector on unix; on Windows -race needs cgo,
# so it runs when gcc is on PATH and falls back to plain `go test` without it)
[unix]
check:
    @go vet ./... && go build ./... && go test -race ./...

[windows]
check:
    @go vet ./... ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    go build ./... ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    if ((Get-Command gcc -ErrorAction SilentlyContinue) -and ($env:CGO_ENABLED -ne '0')) { go test -race ./... } else { echo '== gcc not found: -race needs cgo on Windows - running plain go test (to enable: winget install -e --id BrechtSanders.WinLibs.POSIX.UCRT, then restart the terminal)' ; go test ./... } ; \
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

# measure the deployed stack's sustainable capacity (ramps offered load)
[unix]
bench-calibrate:
    @kubectl -n inference-guard port-forward deploy/gateway 18081:8080 >/dev/null 2>&1 & PF=$! ; sleep 3 ; \
    if ! curl -s -m 3 http://localhost:18081/healthz >/dev/null ; then echo "ERROR: gateway not reachable - run: just up" ; kill $PF 2>/dev/null ; exit 1 ; fi ; \
    : "${BENCH_TENANT_A_KEY:=dev-key-a}" ; export BENCH_TENANT_A_KEY ; \
    : "${BENCH_TENANT_B_KEY:=dev-key-b}" ; export BENCH_TENANT_B_KEY ; \
    go run ./cmd/bench -scenario {{calibration_scenario}} -target http://127.0.0.1:18081 -calibrate ; RC=$? ; \
    kill $PF 2>/dev/null ; exit $RC

[windows]
bench-calibrate:
    @echo '== starting gateway port-forward (18081)' ; \
    $job = Start-Job -ScriptBlock { kubectl -n inference-guard port-forward deploy/gateway 18081:8080 } ; \
    $ok = $false ; \
    foreach ($i in 1..30) { try { Invoke-WebRequest -Uri 'http://localhost:18081/healthz' -TimeoutSec 2 -UseBasicParsing > $null ; $ok = $true ; break } catch { Start-Sleep -Milliseconds 500 } } ; \
    if (-not $ok) { Stop-Job $job -ErrorAction SilentlyContinue ; Remove-Job $job -Force -ErrorAction SilentlyContinue ; echo 'ERROR: gateway not reachable - run: just up' ; exit 1 } ; \
    if (-not $env:BENCH_TENANT_A_KEY) { $env:BENCH_TENANT_A_KEY = 'dev-key-a' } ; \
    if (-not $env:BENCH_TENANT_B_KEY) { $env:BENCH_TENANT_B_KEY = 'dev-key-b' } ; \
    go run ./cmd/bench -scenario {{calibration_scenario}} -target http://127.0.0.1:18081 -calibrate ; $rc = $LASTEXITCODE ; \
    Stop-Job $job -ErrorAction SilentlyContinue ; Remove-Job $job -Force -ErrorAction SilentlyContinue ; \
    if ($rc -ne 0) { exit $rc }

# four control modes (none / static / ratelimit / adaptive) on one calibrated
# 2x-overload workload; restores the gateway env. The bench tool opens its own
# 18081 tunnel for the modes phase, but model discovery + the calibration phase
# talk to -target, so the recipe opens the same 18081 tunnel first (mirrors
# bench-calibrate) - without it bench defaults to 127.0.0.1:8080 and fails.
[unix]
bench-matrix:
    @kubectl -n inference-guard port-forward deploy/gateway 18081:8080 >/dev/null 2>&1 & PF=$! ; sleep 3 ; \
    if ! curl -s -m 3 http://localhost:18081/healthz >/dev/null ; then echo "ERROR: gateway not reachable - run: just up" ; kill $PF 2>/dev/null ; exit 1 ; fi ; \
    : "${BENCH_TENANT_A_KEY:=dev-key-a}" ; export BENCH_TENANT_A_KEY ; \
    : "${BENCH_TENANT_B_KEY:=dev-key-b}" ; export BENCH_TENANT_B_KEY ; \
    go run ./cmd/bench -scenario {{calibration_scenario}} -target http://127.0.0.1:18081 -matrix ; RC=$? ; \
    kill $PF 2>/dev/null ; exit $RC

[windows]
bench-matrix:
    @echo '== starting gateway port-forward (18081, for model discovery + calibration)' ; \
    $job = Start-Job -ScriptBlock { kubectl -n inference-guard port-forward deploy/gateway 18081:8080 } ; \
    $ok = $false ; \
    foreach ($i in 1..30) { try { Invoke-WebRequest -Uri 'http://localhost:18081/healthz' -TimeoutSec 2 -UseBasicParsing > $null ; $ok = $true ; break } catch { Start-Sleep -Milliseconds 500 } } ; \
    if (-not $ok) { Stop-Job $job -ErrorAction SilentlyContinue ; Remove-Job $job -Force -ErrorAction SilentlyContinue ; echo 'ERROR: gateway not reachable - run: just up' ; exit 1 } ; \
    if (-not $env:BENCH_TENANT_A_KEY) { $env:BENCH_TENANT_A_KEY = 'dev-key-a' } ; \
    if (-not $env:BENCH_TENANT_B_KEY) { $env:BENCH_TENANT_B_KEY = 'dev-key-b' } ; \
    echo '== running matrix (calibration first, then 4 modes - this takes a while)' ; \
    go run ./cmd/bench -scenario {{calibration_scenario}} -target http://127.0.0.1:18081 -matrix ; $rc = $LASTEXITCODE ; \
    Stop-Job $job -ErrorAction SilentlyContinue ; Remove-Job $job -Force -ErrorAction SilentlyContinue ; \
    if ($rc -ne 0) { exit $rc }

# dashboards: prints the password, forwards localhost:3000, blocks until Ctrl-C
[unix]
grafana:
    @GPASS=$(kubectl -n monitoring get secret kube-prometheus-stack-grafana -o jsonpath='{.data.admin-password}' | openssl base64 -d -A 2>/dev/null) ; \
    if [ -z "$GPASS" ]; then echo "ERROR: could not read Grafana password - run: just up" ; exit 1 ; fi ; \
    GPORT=$(kubectl -n monitoring get svc kube-prometheus-stack-grafana -o jsonpath='{.spec.ports[0].port}') || exit 1 ; \
    echo "Grafana password: $GPASS" ; \
    echo "Open http://localhost:3000  (Ctrl-C stops the forward)" ; \
    kubectl -n monitoring port-forward svc/kube-prometheus-stack-grafana "3000:$GPORT"

[windows]
grafana:
    @$pwRaw = (kubectl -n monitoring get secret kube-prometheus-stack-grafana -o jsonpath='{.data.admin-password}') ; \
    if (-not $pwRaw) { echo 'ERROR: could not read Grafana password - run: just up' ; exit 1 } ; \
    $GPASS = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($pwRaw)) ; \
    $GPORT = (kubectl -n monitoring get svc kube-prometheus-stack-grafana -o jsonpath='{.spec.ports[0].port}') ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    echo ('Grafana password: ' + $GPASS) ; \
    echo 'Open http://localhost:3000  (Ctrl-C stops the forward)' ; \
    kubectl -n monitoring port-forward svc/kube-prometheus-stack-grafana ('3000:' + $GPORT)

# destroy everything; purges AR images first so destroy stays clean, then
# prints a cost check (disks / repositories / clusters should all be empty)
[unix]
down:
    @echo "== artifact registry purge" ; \
    AR=$(terraform -chdir={{tf_dir}} output -raw artifact_registry_url 2>/dev/null) || AR="" ; \
    if [ -n "$AR" ]; then \
      REGHOST=${AR%%/*} ; REPO=${AR##*/} ; LOC=${REGHOST%%-docker.pkg.dev} ; \
      PROJ=$(printf '%s' "$AR" | sed -E 's#.*pkg\.dev/([^/]+)/.*#\1#') ; \
      echo "== deleting $REPO ($LOC, project $PROJ)" ; \
      gcloud artifacts repositories delete "$REPO" --location="$LOC" --project="$PROJ" --quiet ; \
    fi ; \
    echo "== terraform destroy" ; \
    terraform -chdir={{tf_dir}} destroy -auto-approve ; \
    echo "== cost check - anything listed below would still be billing (all three should say 0 items):" ; \
    gcloud compute disks list ; \
    gcloud artifacts repositories list ; \
    gcloud container clusters list ; \
    exit 0

[windows]
down:
    @echo '== artifact registry purge' ; \
    $AR = (terraform -chdir={{tf_dir}} output -raw artifact_registry_url 2>$null) ; \
    if ($AR) { \
      $RegHost = ($AR -split '/')[0] ; $Repo = ($AR -split '/')[-1] ; $Loc = ($RegHost -replace '-docker\.pkg\.dev$', '') ; $Proj = ($AR -split '/')[1] ; \
      echo ('== deleting ' + $Repo + ' (' + $Loc + ', project ' + $Proj + ')') ; \
      gcloud artifacts repositories delete $Repo --location=$Loc --project=$Proj --quiet ; \
    } ; \
    echo '== terraform destroy' ; \
    terraform -chdir={{tf_dir}} destroy -auto-approve ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    echo '== cost check - anything listed below would still be billing (all three should say 0 items):' ; \
    gcloud compute disks list ; \
    gcloud artifacts repositories list ; \
    gcloud container clusters list ; \
    exit 0

# local kind stack (CPU-sized): strimzi operator + manifests + api keys
[unix]
local-up:
    @kubectl create namespace kafka --dry-run=client -o yaml | kubectl apply -f - && \
    kubectl apply -f "https://strimzi.io/install/latest?namespace=kafka" -n kafka && \
    kubectl apply -k deploy/local && \
    kubectl -n inference-guard create secret generic gateway-api-keys --from-literal="api-keys=dev-key-a=tenant-a,dev-key-b=tenant-b" --dry-run=client -o yaml | kubectl apply -f - && \
    echo "Local stack applied. Next: just gateway-smoke (with the kind context active)"

[windows]
local-up:
    @kubectl create namespace kafka --dry-run=client -o yaml | kubectl apply -f - ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    kubectl apply -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    kubectl apply -k deploy/local ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    kubectl -n inference-guard create secret generic gateway-api-keys --from-literal='api-keys=dev-key-a=tenant-a,dev-key-b=tenant-b' --dry-run=client -o yaml | kubectl apply -f - ; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } ; \
    echo 'Local stack applied. Next: just gateway-smoke (with the kind context active)'
