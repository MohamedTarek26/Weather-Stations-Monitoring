#!/bin/bash
# deploy.sh — One command to deploy the entire weather monitoring system
#
# Usage:
#   ./deploy.sh         Deploy everything
#   ./deploy.sh down    Tear everything down

set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
NAMESPACE="weather-monitoring"

# ─── Tear down ────────────────────────────────────────────────
if [ "$1" = "down" ]; then
    echo "=== Tearing down ==="
    # Kill any port-forward processes
    pkill -f "kubectl port-forward" 2>/dev/null || true
    kubectl delete -f "$PROJECT_DIR/k8s/deployment.yaml" --ignore-not-found
    echo "Done."
    exit 0
fi

# ─── Deploy ───────────────────────────────────────────────────
echo "=== Weather Monitoring System — Full Deploy ==="

# Step 1: Ensure minikube is running
echo "[1/4] Checking minikube..."
if ! minikube status | grep -q "Running"; then
    echo "  Starting minikube..."
    minikube start
fi

# Step 2: Build images inside minikube's Docker (only if needed)
echo "[2/4] Checking Docker images..."
eval $(minikube docker-env 2>/dev/null)

BUILD_NEEDED=false
if [ "$1" = "--build" ]; then
    BUILD_NEEDED=true
    echo "  --build flag: forcing rebuild"
elif ! docker image inspect weather-station:latest &>/dev/null || \
     ! docker image inspect central-station:latest &>/dev/null || \
     ! docker image inspect rain-processor:latest &>/dev/null; then
    BUILD_NEEDED=true
    echo "  Some images missing, building..."
else
    echo "  All images exist. Skipping build. (use './deploy.sh --build' to force)"
fi

if [ "$BUILD_NEEDED" = true ]; then
    docker build --no-cache -t weather-station:latest -f "$PROJECT_DIR/weather-station/Dockerfile" "$PROJECT_DIR"
    docker build --no-cache -t central-station:latest -f "$PROJECT_DIR/central-station/Dockerfile" "$PROJECT_DIR"
    docker build --no-cache -t rain-processor:latest  -f "$PROJECT_DIR/rain-processor/Dockerfile"  "$PROJECT_DIR"
fi

# Step 3: Deploy to K8s
echo "[3/4] Deploying to Kubernetes..."
kubectl apply -f "$PROJECT_DIR/k8s/deployment.yaml"

# Step 4: Wait for all pods to be ready
echo "[4/4] Waiting for pods..."
kubectl wait --for=condition=ready pod --all -n $NAMESPACE --timeout=120s

echo ""
echo "=== All pods running! ==="
kubectl get pods -n $NAMESPACE
echo ""

# Print access URLs using NodePort
MINIKUBE_IP=$(minikube ip)

# Import Kibana dashboards if file exists
DASHBOARDS_FILE="$PROJECT_DIR/kibana_dashboards.ndjson"
if [ -f "$DASHBOARDS_FILE" ]; then
    echo "=== Importing Kibana dashboards ==="
    echo "  Waiting for Kibana to be ready..."
    for i in $(seq 1 30); do
        if curl -s "http://$MINIKUBE_IP:30561/api/status" 2>/dev/null | grep -q '"level":"available"'; then
            echo "  Kibana is ready!"
            curl -s -X POST "http://$MINIKUBE_IP:30561/api/saved_objects/_import?overwrite=true" \
                -H "kbn-xsrf: true" \
                --form file=@"$DASHBOARDS_FILE" | python3 -c "import json,sys; d=json.load(sys.stdin); print(f'  Imported {d.get(\"successCount\",0)} objects')" 2>/dev/null
            break
        fi
        echo -n "."
        sleep 3
    done
fi

echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║  Kibana:          http://$MINIKUBE_IP:30561  "
echo "║  Central Station: http://$MINIKUBE_IP:30080  "
echo "║                                              ║"
echo "║  Logs:  kubectl logs -f deployment/central-station -n $NAMESPACE"
echo "║  Stop:  ./deploy.sh down                     ║"
echo "╚══════════════════════════════════════════════╝"
