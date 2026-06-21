#!/usr/bin/env bash
# scripts/integration/flushnow-test.sh
#
# Black-box integration test: verify a deployed micewriter engine pod responds
# correctly to the FlushNow gRPC RPC (micewriter.v2.Micewriter/FlushNow).
#
# Topology assumptions:
#   - k3s cluster provisioned by k3sonhyperv
#   - Nessie + MinIO installed into $NAMESPACE by micewriter-local-infra
#     ("cd ../micewriter-local-infra && pwsh ./run.ps1 up")
#   - Engine deployed via the table-pipeline Helm chart with enableManualFlush=true
#     ("helm upgrade --install engine-telemetry-events
#       ../micewriter-local-infra/charts/table-pipeline
#       --set table=telemetry_events -n micewriter-infra --wait")
#
# Required tools: grpcurl, kubectl, jq
#   grpcurl install (static binary, no deps):
#     GRPCURL_VER=1.9.1
#     curl -sSL https://github.com/fullstorydev/grpcurl/releases/download/v${GRPCURL_VER}/grpcurl_${GRPCURL_VER}_linux_x86_64.tar.gz \
#       | tar -xz -C ~/.local/bin grpcurl
#
# Overridable env vars:
#   TABLE         Iceberg table name (default: telemetry_events)
#   NAMESPACE     Kubernetes namespace (default: micewriter-infra)
#   GRPC_PORT     gRPC port on the service (default: 9090)
#   LOCAL_PORT    Local port for kubectl port-forward (default: 9090)
#   PROTO_DIR     Directory containing micewriter.proto (auto-resolved from script location)

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

TABLE="${TABLE:-telemetry_events}"
NAMESPACE="${NAMESPACE:-micewriter-infra}"
GRPC_PORT="${GRPC_PORT:-9090}"
LOCAL_PORT="${LOCAL_PORT:-9090}"

# Derive service name: engine-{table with underscores replaced by dashes}
SVC="engine-${TABLE//_/-}"

# Proto lives in the sibling SDK repo: scripts/integration/../../.. => engine root => ../ => repos root
PROTO_DIR="${PROTO_DIR:-"${SCRIPT_DIR}/../../../micewriter-sdk-java/micewriter-sdk-java-core/src/main/proto"}"

# State
PF_PID=""
RESULT=0

cleanup() {
    if [[ -n "$PF_PID" ]] && kill -0 "$PF_PID" 2>/dev/null; then
        kill "$PF_PID" 2>/dev/null || true
        wait "$PF_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

log()  { echo "  $*"; }
pass() { echo; echo "✓ PASS: $*"; }
fail() { echo; echo "✗ FAIL: $*"; RESULT=1; }

echo "=== micewriter FlushNow integration test ==="
echo "  table:      ${TABLE}"
echo "  service:    ${SVC}.${NAMESPACE}:${GRPC_PORT}"
echo "  local port: ${LOCAL_PORT}"
echo "  proto dir:  ${PROTO_DIR}"
echo

# ---------------------------------------------------------------------------
# 1. Preflight: required tools
# ---------------------------------------------------------------------------
echo "--- [1/5] Preflight: tools ---"

MISSING_TOOLS=()
for tool in grpcurl kubectl jq; do
    if command -v "$tool" &>/dev/null; then
        log "$tool: $(command -v "$tool")"
    else
        log "$tool: MISSING"
        MISSING_TOOLS+=("$tool")
    fi
done

if [[ "${#MISSING_TOOLS[@]}" -gt 0 ]]; then
    echo
    echo "ERROR: missing required tool(s): ${MISSING_TOOLS[*]}"
    if [[ " ${MISSING_TOOLS[*]} " == *" grpcurl "* ]]; then
        echo
        echo "  Install grpcurl (static binary, no dependencies):"
        echo "    GRPCURL_VER=1.9.1"
        echo '    curl -sSL "https://github.com/fullstorydev/grpcurl/releases/download/v${GRPCURL_VER}/grpcurl_${GRPCURL_VER}_linux_x86_64.tar.gz" \'
        echo '      | tar -xz -C ~/.local/bin grpcurl'
        echo '    chmod +x ~/.local/bin/grpcurl'
        echo "    # Ensure ~/.local/bin is on your PATH"
    fi
    exit 1
fi

if [[ ! -f "${PROTO_DIR}/micewriter.proto" ]]; then
    echo
    echo "ERROR: proto not found at ${PROTO_DIR}/micewriter.proto"
    echo "  Ensure micewriter-sdk-java is checked out as a sibling of micewriter-engine,"
    echo "  or override: PROTO_DIR=/path/to/proto/dir $0"
    exit 1
fi
log "proto: ${PROTO_DIR}/micewriter.proto"

# ---------------------------------------------------------------------------
# 2. Preflight: cluster reachable
# ---------------------------------------------------------------------------
echo
echo "--- [2/5] Preflight: cluster ---"

if ! kubectl cluster-info &>/dev/null; then
    echo
    echo "ERROR: kubectl cannot reach the cluster."
    echo "  Is the k3sonhyperv cluster running? Check: kubectl cluster-info"
    exit 1
fi
log "cluster: reachable"

# ---------------------------------------------------------------------------
# 3. Preflight: MinIO + Nessie pods Running
# ---------------------------------------------------------------------------
echo
echo "--- [3/5] Preflight: infra pods ($NAMESPACE) ---"

for component in minio nessie; do
    RUNNING=$(kubectl get pods -n "$NAMESPACE" \
        --field-selector=status.phase=Running \
        -o jsonpath='{.items[*].metadata.name}' 2>/dev/null \
        | tr ' ' '\n' | grep -c "$component" || true)
    if [[ "$RUNNING" -eq 0 ]]; then
        echo
        echo "ERROR: no Running pod matching '${component}' in namespace ${NAMESPACE}."
        echo "  Start the local infra stack:"
        echo "    cd ../micewriter-local-infra && pwsh ./run.ps1 up"
        echo "  (or: make up)"
        exit 1
    fi
    log "${component}: ${RUNNING} running pod(s)"
done

# ---------------------------------------------------------------------------
# 4. Preflight: engine deployment ready
# ---------------------------------------------------------------------------
echo
echo "--- [4/5] Preflight: engine deployment ($SVC) ---"

if ! kubectl get deploy "$SVC" -n "$NAMESPACE" &>/dev/null; then
    echo
    echo "ERROR: Deployment '${SVC}' not found in namespace ${NAMESPACE}."
    echo "  Build/push the image (push.ps1) and deploy:"
    echo "    helm upgrade --install ${SVC} ../micewriter-local-infra/charts/table-pipeline \\"
    echo "      --set table=${TABLE} -n ${NAMESPACE} --wait"
    exit 1
fi

log "waiting for rollout to complete..."
kubectl rollout status "deploy/$SVC" -n "$NAMESPACE" --timeout=120s >/dev/null
log "deployment: ready"

# ---------------------------------------------------------------------------
# 5. Port-forward + invoke + assert
# ---------------------------------------------------------------------------
echo
echo "--- [5/5] FlushNow RPC ---"

# Check the local port is not already occupied
if (echo >/dev/tcp/localhost/"$LOCAL_PORT") 2>/dev/null; then
    echo
    echo "ERROR: local port ${LOCAL_PORT} is already in use."
    echo "  Override: LOCAL_PORT=19090 $0"
    exit 1
fi

log "starting port-forward: svc/${SVC} ${LOCAL_PORT}:${GRPC_PORT} ..."
kubectl port-forward "svc/$SVC" "${LOCAL_PORT}:${GRPC_PORT}" \
    -n "$NAMESPACE" &>/dev/null &
PF_PID=$!

# Wait until the local port accepts TCP connections (engine has no readiness probe)
WAIT_DEADLINE=30   # seconds
HALF_SECOND_TICKS=$((WAIT_DEADLINE * 2))
log "waiting up to ${WAIT_DEADLINE}s for localhost:${LOCAL_PORT} ..."
READY=0
for i in $(seq 1 "$HALF_SECOND_TICKS"); do
    if (echo >/dev/tcp/localhost/"$LOCAL_PORT") 2>/dev/null; then
        READY=1
        break
    fi
    sleep 0.5
done

if [[ "$READY" -eq 0 ]]; then
    echo
    echo "ERROR: timed out waiting for port-forward on localhost:${LOCAL_PORT}."
    echo "  Try: LOCAL_PORT=19090 $0"
    exit 1
fi
log "port-forward: ready"

log "invoking micewriter.v2.Micewriter/FlushNow  {table: \"${TABLE}\"} ..."
RESPONSE=$(grpcurl \
    -plaintext \
    -import-path "$PROTO_DIR" \
    -proto micewriter.proto \
    -d "{\"table\":\"${TABLE}\"}" \
    "localhost:${LOCAL_PORT}" \
    micewriter.v2.Micewriter/FlushNow 2>&1) || {
    echo
    echo "ERROR: grpcurl call failed:"
    echo "$RESPONSE"
    exit 1
}
log "response: ${RESPONSE}"

# Parse and assert
OK=$(echo  "$RESPONSE" | jq -r '.ok'      2>/dev/null || echo "parse-error")
MSG=$(echo "$RESPONSE" | jq -r '.message' 2>/dev/null || echo "parse-error")

if [[ "$OK" == "true" ]] && [[ "$MSG" == *"Flush triggered"* ]]; then
    pass "FlushNow returned ok=true, message=\"${MSG}\""
elif [[ "$MSG" == *"disabled"* ]]; then
    fail "Manual flush is disabled on the pod (ENABLE_MANUAL_FLUSH=false)."
    echo "      Re-deploy with:"
    echo "        helm upgrade ${SVC} ../micewriter-local-infra/charts/table-pipeline \\"
    echo "          --set table=${TABLE} --set enableManualFlush=true -n ${NAMESPACE} --wait"
else
    fail "Unexpected response — ok=${OK}, message=\"${MSG}\""
fi

exit $RESULT
